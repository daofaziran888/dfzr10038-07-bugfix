"""跨模块真实数据流编排：在原子动作基础上建立业务链路消费关系。

每条链路封装为 FlowStep，由 FlowEngine 按序执行并收集副作用。
不做独立CRUD，只在业务动作完成后触发下游消费逻辑。
"""
from __future__ import annotations

import datetime
import math
import random
from dataclasses import dataclass, field
from typing import Any, Callable

from app.store import store

# ---------------------------------------------------------------------------
# 链路副作用记录
# ---------------------------------------------------------------------------


@dataclass
class FlowEffect:
    """单次链路触发产生的副作用记录。"""
    name: str
    module: str
    entry_id: int
    description: str
    timestamp: str = field(default_factory=lambda: datetime.datetime.now().isoformat())


# ---------------------------------------------------------------------------
# 链路定义：每条链路 = path + trigger 函数
# ---------------------------------------------------------------------------

FLOW_LINKS: list[dict[str, Any]] = []


def _register(path: str, *, desc: str):
    """装饰器工厂：在 FLOW_LINKS 中注册链路定义。"""
    def decorator(trigger: Callable[..., list[FlowEffect]]):
        FLOW_LINKS.append({"path": path, "trigger": trigger, "desc": desc})
        return trigger
    return decorator


# ============================ 链路1: 特征注册→衍生工程→时间窗口 ============================


@_register(
    "feature_reg/审核注册 → derive_rule/启效规则 → time_window/启动计算",
    desc="特征注册后自动触发对应衍生规则生效、时间窗口模板计算",
)
def flow_reg_to_derive_to_window(feature_reg_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    feature_reg_entry = store.find("feature_reg", feature_reg_id)
    if feature_reg_entry is None or feature_reg_entry.get("status") != "已注册":
        return effects

    feat_no = str(feature_reg_entry.get("特征编号", ""))
    feat_name = str(feature_reg_entry.get("特征名称", ""))

    # 查找关联的衍生规则并启效
    derive_rows = store.rows("derive_rule")
    for row in derive_rows:
        if str(row.get("输入特征", "")) and feat_name in str(row.get("输入特征", "")):
            if row.get("status") == "草稿":
                row["status"] = "生效中"
                row["pending"] = True
                row["abnormal"] = False
                effects.append(FlowEffect(
                    "启效规则", "derive_rule", int(row.get("id", 0)),
                    f"特征 {feat_no} 注册后联动启效衍生规则 {row.get('规则编号','')}",
                ))
                # 同时联动时间窗口
                _window_compute_for_derived(row, feat_no, effects)

    return effects


def _window_compute_for_derived(
    derive_row: dict[str, Any], source_feat_no: str, effects: list[FlowEffect]
):
    """为已启效的衍生规则关联的时间窗口触发计算。"""
    win_rows = store.rows("time_window")
    output_feat = str(derive_row.get("输出特征", ""))
    for wrow in win_rows:
        if wrow.get("status") == "草稿":
            wrow["status"] = "计算中"
            wrow["pending"] = True
            wrow["awakened_by"] = f"feature_reg/{source_feat_no}"
            effects.append(FlowEffect(
                "启动计算", "time_window", int(wrow.get("id", 0)),
                f"衍生规则 {derive_row.get('规则编号','')} 启效联动窗口 {wrow.get('模板编号','')} 启动计算",
            ))


# ============================ 链路2: 时间窗口→特征回溯→特征存储/监控 ============================


@_register(
    "time_window/窗口关闭 → backfill_job/启动回溯 → feature_store/启用存储 → feature_mon/启用监控",
    desc="窗口关闭后发起特征回溯，回溯完成写入存储并启用监控。",
)
def flow_window_to_backfill_to_store_mon(time_window_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    win = store.find("time_window", time_window_id)
    if win is None or win.get("status") != "窗口关闭":
        return effects

    template_no = str(win.get("模板编号", ""))
    feat_list = str(win.get("特征列表", ""))

    # 触发回溯任务
    backfill_rows = store.rows("backfill_job")
    for brow in backfill_rows:
        if brow.get("status") == "待执行":
            brow["status"] = "回溯中"
            brow["pending"] = True
            brow["triggered_by"] = f"time_window/{template_no}"
            brow["当前进度"] = "0%"
            effects.append(FlowEffect(
                "启动回溯", "backfill_job", int(brow.get("id", 0)),
                f"窗口 {template_no} 关闭后触发回溯任务 {brow.get('任务编号','')}",
            ))
            # 模拟回溯完成 → 触发存储写入
            _backfill_done_store_mon(brow, effects)
            break

    return effects


def _backfill_done_store_mon(backfill_row: dict[str, Any], effects: list[FlowEffect]):
    """回溯完成：触发特征存储启用和监控规则检测。"""
    backfill_row["status"] = "已完成"
    backfill_row["pending"] = False
    backfill_row["当前进度"] = "100%"

    job_no = str(backfill_row.get("任务编号", ""))
    target = str(backfill_row.get("目标特征", ""))
    row_count = random.randint(50000, 500000)
    backfill_row["数据行数"] = f"{row_count}行"

    effects.append(FlowEffect(
        "回溯完成", "backfill_job", int(backfill_row.get("id", 0)),
        f"回溯任务 {job_no} 完成，产出 {row_count} 行，触发存储写入和监控检测",
    ))

    # 触发特征存储写入
    store_rows = store.rows("feature_store")
    for srow in store_rows:
        if srow.get("status") == "草稿":
            srow["status"] = "已启用"
            srow["pending"] = True
            srow["storage_size_gb"] = round(row_count * 0.0001, 2)
            srow["triggered_by"] = f"backfill_job/{job_no}"
            effects.append(FlowEffect(
                "启用存储", "feature_store", int(srow.get("id", 0)),
                f"回溯 {job_no} 产出 {row_count}行 触发存储配置 {srow.get('配置编号','')} 启用",
            ))
            break

    # 触发特征监控检测
    mon_rows = store.rows("feature_mon")
    for mrow in mon_rows:
        if mrow.get("status") == "草稿" and target in str(mrow.get("目标特征", "")):
            mrow["status"] = "监控中"
            mrow["pending"] = True
            mrow["最近检测"] = datetime.date.today().isoformat()
            mrow["触发源"] = f"backfill_job/{job_no}"
            # 随机模拟漂移检测
            drift = random.uniform(0, 0.15)
            if drift > 0.1:
                mrow["status"] = "分布漂移"
                mrow["漂移方向"] = "正向偏移"
            effects.append(FlowEffect(
                "启用监控/检测漂移", "feature_mon", int(mrow.get("id", 0)),
                f"回溯 {job_no} 完成触发监控 {mrow.get('规则编号','')} 检测，漂移={drift:.3f}",
            ))
            break


# ============================ 链路3: 特征存储→在线服务/离线导出 ============================


@_register(
    "feature_store/启用存储 → online_serv/部署服务 → offline_exp/启动导出",
    desc="特征存储启用后同步部署在线服务和触发离线导出管道。",
)
def flow_store_to_online_offline(feature_store_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    fs = store.find("feature_store", feature_store_id)
    if fs is None or fs.get("status") != "已启用":
        return effects

    config_no = str(fs.get("配置编号", ""))

    # 触发在线服务部署
    online_rows = store.rows("online_serv")
    for orow in online_rows:
        if orow.get("status") == "待部署":
            orow["status"] = "运行中"
            orow["pending"] = True
            orow["热加载"] = "已开启"
            orow["运行状态"] = "SERVING"
            orow["triggered_by"] = f"feature_store/{config_no}"
            effects.append(FlowEffect(
                "部署服务", "online_serv", int(orow.get("id", 0)),
                f"存储 {config_no} 启用后自动部署在线服务 {orow.get('端点编号','')}",
            ))
            break

    # 触发离线导出
    offline_rows = store.rows("offline_exp")
    for erow in offline_rows:
        if erow.get("status") == "待执行":
            erow["status"] = "导出中"
            erow["pending"] = True
            erow["最后导出"] = datetime.datetime.now().isoformat()
            erow["triggered_by"] = f"feature_store/{config_no}"
            effects.append(FlowEffect(
                "启动导出", "offline_exp", int(erow.get("id", 0)),
                f"存储 {config_no} 启用后联动离线导出管道 {erow.get('管道编号','')}",
            ))
            break

    return effects


# ============================ 链路4: 衍生规则→特征血缘 ============================


@_register(
    "derive_rule/启效规则 → feature_line/确认链路",
    desc="衍生规则生效后自动推导并确认特征血缘链路。",
)
def flow_derive_to_lineage(derive_rule_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    dr = store.find("derive_rule", derive_rule_id)
    if dr is None or dr.get("status") != "生效中":
        return effects

    rule_no = str(dr.get("规则编号", ""))
    input_feat = str(dr.get("输入特征", ""))
    output_feat = str(dr.get("输出特征", ""))
    func_name = str(dr.get("衍生函数", ""))

    # 自动推导血缘并确认
    line_rows = store.rows("feature_line")
    for lrow in line_rows:
        if lrow.get("status") == "自动推导":
            lrow["status"] = "已确认"
            lrow["pending"] = False
            lrow["上游特征"] = input_feat
            lrow["下游特征"] = output_feat
            lrow["转换函数"] = func_name
            lrow["依赖深度"] = "1"
            lrow["最后更新"] = datetime.datetime.now().isoformat()
            lrow["triggered_by"] = f"derive_rule/{rule_no}"
            effects.append(FlowEffect(
                "确认链路", "feature_line", int(lrow.get("id", 0)),
                f"衍生规则 {rule_no} 启效后自动推导血缘 {input_feat}→{output_feat}",
            ))
            break

    return effects


# ============================ 链路5: 特征血缘→版本管理→特征监控 ============================


@_register(
    "feature_line/确认链路 → version_mgr/灰度发布 → feature_mon/启用监控",
    desc="血缘确认后触发版本灰度发布，并关联启用特征监控规则。",
)
def flow_lineage_to_version_to_mon(feature_line_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    fl = store.find("feature_line", feature_line_id)
    if fl is None or fl.get("status") != "已确认":
        return effects

    line_no = str(fl.get("链路编号", ""))
    downstream = str(fl.get("下游特征", ""))

    # 版本灰度发布
    ver_rows = store.rows("version_mgr")
    for vrow in ver_rows:
        if vrow.get("status") == "草稿" and downstream in str(vrow.get("关联特征", "")):
            vrow["status"] = "灰度中"
            vrow["pending"] = True
            vrow["灰度比例"] = "30%"
            vrow["版本状态"] = "灰度测试中"
            vrow["triggered_by"] = f"feature_line/{line_no}"
            effects.append(FlowEffect(
                "灰度发布", "version_mgr", int(vrow.get("id", 0)),
                f"血缘 {line_no} 确认后触发版本 {vrow.get('版本编号','')} 灰度发布",
            ))
            break

    # 启用监控
    mon_rows = store.rows("feature_mon")
    for mrow in mon_rows:
        if mrow.get("status") == "草稿" and downstream in str(mrow.get("目标特征", "")):
            mrow["status"] = "监控中"
            mrow["pending"] = True
            mrow["最近检测"] = datetime.date.today().isoformat()
            mrow["triggered_by"] = f"feature_line/{line_no}"
            effects.append(FlowEffect(
                "启用监控", "feature_mon", int(mrow.get("id", 0)),
                f"血缘 {line_no} 确认后启用监控 {mrow.get('规则编号','')}",
            ))
            break

    return effects


# ============================ 链路6: 版本管理→在线服务 ============================
#
# 版本侧(version_mgr)是特征绑定的唯一事实来源，端侧(online_serv)只保存
# 当前有效绑定的投影：
#   - 端点 version_bindings：{关联特征: {版本编号, 版本标签, tag}}，每特征至多一个生效版本；
#   - 端点 特征列表：普通特征 token + 有效绑定标签，已撤销/被替换的标签一律剔除；
#   - 端点 当前生效版本：值班员据此判断端点是否已换版、能否开始下一次切换；
#   - 版本 生效端点 / 操作记录：切换与回滚各自的结果都能从版本侧回查。


def _version_tag(version_row: dict[str, Any]) -> str:
    """版本在端点特征列表中的标签 token。"""
    return f"v{version_row.get('版本标签', '')}"


def _known_version_tags() -> set[str]:
    """所有版本行产生过的标签，用于识别特征列表里的版本标签 token。"""
    return {
        _version_tag(row)
        for row in store.rows("version_mgr")
        if str(row.get("版本标签", "")).strip()
    }


def _endpoint_bindings(orow: dict[str, Any]) -> dict[str, dict[str, str]]:
    """端点上每个特征当前绑定的生效版本：{关联特征: {版本编号, 版本标签, tag}}。"""
    bindings = orow.get("version_bindings")
    if not isinstance(bindings, dict):
        bindings = {}
        orow["version_bindings"] = bindings
    return bindings


def _resolve_effective_version(feature: str) -> dict[str, Any] | None:
    """同一特征当前应生效的版本：仍处于全量生效且最近切换的一条。

    回滚 B 时用它找回 A；已回滚的版本永远不会被选中，
    因此热加载失败后重试也不会把已撤销的标签重新挂回端点。
    """
    candidates = [
        row for row in store.rows("version_mgr")
        if str(row.get("关联特征", "")) == feature and row.get("status") == "全量生效"
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (str(row.get("last_full_switch_at", "")), int(row.get("id", 0))),
    )


def _rewrite_feature_list(orow: dict[str, Any]) -> bool:
    """按当前有效绑定重建端点对外可读的特征清单。

    - 普通特征 token 原样保留（兼容未涉及版本流转的历史绑定）；
    - 仍是有效绑定的版本标签保留，且同一标签不重复出现；
    - 已撤销或已被替换的版本标签一律剔除。
    """
    tokens = [t for t in str(orow.get("特征列表", "")).split(",") if t.strip()]
    known_tags = _known_version_tags()
    base = [t for t in tokens if t not in known_tags]
    tags: list[str] = []
    for binding in _endpoint_bindings(orow).values():
        tag = str(binding.get("tag", ""))
        if tag and tag not in tags:
            tags.append(tag)
    new_list = ",".join(base + tags)
    changed = new_list != str(orow.get("特征列表", ""))
    orow["特征列表"] = new_list
    return changed


def _refresh_endpoint_version_view(orow: dict[str, Any]) -> None:
    """把端点实际生效的版本写到在线清单，值班员据此判断能否开始下一次切换。"""
    bindings = _endpoint_bindings(orow)
    orow["当前生效版本"] = (
        ";".join(
            f"{feat}={binding.get('版本编号', '')}({binding.get('tag', '')})"
            for feat, binding in bindings.items()
        )
        if bindings else "无"
    )


def _sync_version_endpoint_index(feature: str) -> None:
    """以端点绑定为唯一事实，回写同特征各版本当前生效的端点清单。

    切换和回滚并发时，版本侧与端点侧据此收敛到同一份相符的端点清单。
    """
    for vrow in store.rows("version_mgr"):
        if str(vrow.get("关联特征", "")) != feature:
            continue
        ver_no = str(vrow.get("版本编号", ""))
        vrow["生效端点"] = [
            str(orow.get("端点编号", ""))
            for orow in store.rows("online_serv")
            if _endpoint_bindings(orow).get(feature, {}).get("版本编号") == ver_no
        ]


def _record_version_receipt(
    vrow: dict[str, Any], action: str, affected: list[dict[str, Any]], note: str,
) -> None:
    """在版本侧追加操作回执，切换/回滚各自的结果都能从版本侧回查。

    幂等重试或 flow-trace 预览没有实际影响端点时，不重复登记空回执。
    """
    receipts = vrow.setdefault("操作记录", [])
    if not affected and any(r.get("动作") == action for r in receipts):
        return
    receipts.append({
        "动作": action,
        "影响端点": [str(o.get("端点编号", "")) for o in affected],
        "结果": note,
        "时间": datetime.datetime.now().isoformat(),
    })


@_register(
    "version_mgr/全量切换 → online_serv/部署服务(热加载)",
    desc="版本全量切换后触发在线服务热加载，更新服务端点特征列表。",
)
def flow_version_to_online(version_mgr_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    vm = store.find("version_mgr", version_mgr_id)
    # 已回滚（或尚未全量）的版本不参与热加载：
    # 热加载失败后重试也不会把已撤销的标签重新挂回端点。
    if vm is None or vm.get("status") != "全量生效":
        return effects

    ver_no = str(vm.get("版本编号", ""))
    feat = str(vm.get("关联特征", ""))
    ver_label = str(vm.get("版本标签", ""))
    tag = _version_tag(vm)
    now = datetime.datetime.now().isoformat()
    vm["版本状态"] = "全量生效"

    # 触发在线服务热加载：同特征旧版本标签被替换，其它特征的历史绑定保持不变
    online_rows = store.rows("online_serv")
    affected: list[dict[str, Any]] = []
    running = [orow for orow in online_rows if orow.get("status") == "运行中"]
    for orow in running:
        bindings = _endpoint_bindings(orow)
        previous = bindings.get(feat)
        new_binding = {"版本编号": ver_no, "版本标签": ver_label, "tag": tag}
        changed = previous != new_binding
        bindings[feat] = new_binding
        changed = _rewrite_feature_list(orow) or changed
        if not changed:
            continue  # 幂等：该端点已是目标绑定，重试不重复挂标签
        _refresh_endpoint_version_view(orow)
        orow["热加载"] = f"已重载({ver_no})"
        orow["last_version_switch"] = now
        orow["triggered_by"] = f"version_mgr/{ver_no}"
        prev_desc = f"，替换 {previous.get('版本编号','')}" if previous else ""
        effects.append(FlowEffect(
            "热加载服务", "online_serv", int(orow.get("id", 0)),
            f"版本 {ver_no}({ver_label}) 全量切换后热加载在线服务 "
            f"{orow.get('端点编号','')}{prev_desc}，当前生效 {ver_no}({tag})",
        ))
        affected.append(orow)

    if not running:
        # 如果无运行中端点则部署新端点
        for orow in online_rows:
            if orow.get("status") == "待部署":
                orow["status"] = "运行中"
                orow["pending"] = True
                orow["热加载"] = "已开启"
                orow["运行状态"] = "SERVING"
                orow["特征列表"] = feat
                _endpoint_bindings(orow)[feat] = {
                    "版本编号": ver_no, "版本标签": ver_label, "tag": tag,
                }
                _rewrite_feature_list(orow)
                _refresh_endpoint_version_view(orow)
                orow["triggered_by"] = f"version_mgr/{ver_no}"
                effects.append(FlowEffect(
                    "部署服务", "online_serv", int(orow.get("id", 0)),
                    f"版本 {ver_no} 全量切换后部署在线服务 "
                    f"{orow.get('端点编号','')}，当前生效 {ver_no}({tag})",
                ))
                affected.append(orow)
                break

    if affected or not vm.get("last_full_switch_at"):
        vm["last_full_switch_at"] = now
    _sync_version_endpoint_index(feat)
    note = (
        f"当前生效版本 {ver_no}({tag})" if affected
        else "无运行中或待部署端点可热加载"
    )
    _record_version_receipt(vm, "全量切换", affected, note)
    return effects


@_register(
    "version_mgr/回滚版本 → online_serv/热加载回滚(撤销标签)",
    desc="版本回滚后在线服务按版本侧结果撤销标签，端点特征明细恢复到有效绑定。",
)
def flow_version_rollback_to_online(version_mgr_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    vm = store.find("version_mgr", version_mgr_id)
    if vm is None or vm.get("status") != "已回滚":
        return effects

    ver_no = str(vm.get("版本编号", ""))
    feat = str(vm.get("关联特征", ""))
    ver_label = str(vm.get("版本标签", ""))
    tag = _version_tag(vm)
    now = datetime.datetime.now().isoformat()
    vm["版本状态"] = "已回滚"

    # 回滚确认后在线查询只能看到有效绑定：同特征若仍有全量生效的旧版本则回挂，
    # 否则该特征在端点上不再绑定任何版本。
    successor = _resolve_effective_version(feat)
    successor_desc = (
        f"{successor.get('版本编号', '')}({_version_tag(successor)})"
        if successor else "无"
    )

    affected: list[dict[str, Any]] = []
    for orow in store.rows("online_serv"):
        bindings = _endpoint_bindings(orow)
        bound = bindings.get(feat)
        tag_present = tag in str(orow.get("特征列表", "")).split(",")
        involved = (bound is not None and bound.get("版本编号") == ver_no) or tag_present
        if not involved:
            continue  # 未涉及本次回滚的端点与特征保持原样
        if bound is not None and bound.get("版本编号") == ver_no:
            if successor is not None:
                bindings[feat] = {
                    "版本编号": str(successor.get("版本编号", "")),
                    "版本标签": str(successor.get("版本标签", "")),
                    "tag": _version_tag(successor),
                }
            else:
                bindings.pop(feat, None)
        _rewrite_feature_list(orow)  # 已撤销标签随重建剔除，历史有效绑定保留
        _refresh_endpoint_version_view(orow)
        orow["热加载"] = f"已回滚({ver_no})"
        orow["last_version_rollback"] = now
        orow["triggered_by"] = f"version_mgr/{ver_no}"
        effects.append(FlowEffect(
            "回滚热加载", "online_serv", int(orow.get("id", 0)),
            f"版本 {ver_no}({ver_label}) 回滚后撤销在线服务 "
            f"{orow.get('端点编号','')} 的标签 {tag}，当前生效版本 {successor_desc}",
        ))
        affected.append(orow)

    _sync_version_endpoint_index(feat)
    note = (
        f"已撤销标签 {tag}，端点当前生效版本 {successor_desc}" if affected
        else "无在线端点绑定该版本，无需热加载回滚"
    )
    _record_version_receipt(vm, "回滚版本", affected, note)
    return effects


# ============================ 链路7: 样本分层→标注派发→标注审核→一致性计算 ============================


@_register(
    "sample_strata/已出样 → label_task/派发任务 → label_review/确认通过 → consent_calc/启动计算",
    desc="样本确认出样后派发标注任务，审核通过后自动触发一致性计算。",
)
def flow_sample_to_label_to_review_to_consent(sample_strata_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    ss = store.find("sample_strata", sample_strata_id)
    if ss is None or ss.get("status") != "已出样":
        return effects

    strategy_no = str(ss.get("策略编号", ""))
    sample_count = int(ss.get("样本数量", 10))

    # 标注派发
    label_task = _dispatch_label_task(strategy_no, sample_count, effects)

    if label_task is None:
        return effects

    # 模拟标注完成 → 审核通过
    _label_review_pass(label_task, effects)
    # 审核通过 → 一致性计算
    _consent_calc_trigger(label_task, effects)

    return effects


def _dispatch_label_task(
    strategy_no: str, sample_count: int, effects: list[FlowEffect]
) -> dict[str, Any] | None:
    """派发标注任务。"""
    label_rows = store.rows("label_task")
    for lrow in label_rows:
        if lrow.get("status") == "待派发":
            lrow["status"] = "标注中"
            lrow["pending"] = True
            lrow["样本范围"] = f"样本{strategy_no}"
            lrow["完成进度"] = "100%"
            lrow["派发时间"] = datetime.datetime.now().isoformat()
            lrow["triggered_by"] = f"sample_strata/{strategy_no}"
            effects.append(FlowEffect(
                "派发任务", "label_task", int(lrow.get("id", 0)),
                f"样本 {strategy_no}({sample_count}条) 出样后派发标注任务 {lrow.get('任务编号','')}",
            ))
            return lrow
    return None


def _label_review_pass(label_task: dict[str, Any], effects: list[FlowEffect]):
    """标注审核通过。"""
    task_no = str(label_task.get("任务编号", ""))
    review_rows = store.rows("label_review")
    for rrow in review_rows:
        if rrow.get("status") == "待审核":
            rrow["status"] = "已通过"
            rrow["pending"] = False
            rrow["标注任务"] = task_no
            rrow["样本数量"] = 30
            rrow["通过数量"] = 29
            rrow["驳回数量"] = 1
            rrow["一致率"] = "96.7%"
            rrow["审核时间"] = datetime.datetime.now().isoformat()
            rrow["triggered_by"] = f"label_task/{task_no}"
            effects.append(FlowEffect(
                "确认通过", "label_review", int(rrow.get("id", 0)),
                f"标注 {task_no} 审核通过，一致率 {rrow['一致率']}，触发一致性计算",
            ))
            break


def _consent_calc_trigger(label_task: dict[str, Any], effects: list[FlowEffect]):
    """审核通过后触发一致性计算。"""
    task_no = str(label_task.get("任务编号", ""))
    consent_rows = store.rows("consent_calc")
    for crow in consent_rows:
        if crow.get("status") == "待计算":
            crow["status"] = "已出分"
            crow["pending"] = False
            crow["标注任务"] = task_no
            kappa = round(random.uniform(0.75, 0.95), 3)
            crow["Kappa系数"] = str(kappa)
            crow["一致率"] = f"{round(kappa * 100, 1)}%"
            crow["评估结论"] = "高一致性" if kappa > 0.8 else "中等一致性"
            crow["计算时间"] = datetime.datetime.now().isoformat()
            crow["triggered_by"] = f"label_review/{task_no}"
            effects.append(FlowEffect(
                "启动计算", "consent_calc", int(crow.get("id", 0)),
                f"审核 {task_no} 通过后计算一致性，Kappa={kappa}",
            ))
            break


# ============================ 链路8: 特征目录→治理审计→特征报告 ============================


@_register(
    "feat_catalog/发布目录 → govern_audit/发起审计 → report_task/启动生成",
    desc="特征目录发布后触发治理审计，审计完成后生成特征报告。",
)
def flow_catalog_to_audit_to_report(feat_catalog_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    fc = store.find("feat_catalog", feat_catalog_id)
    if fc is None or fc.get("status") != "已发布":
        return effects

    index_no = str(fc.get("索引编号", ""))
    catalog_name = str(fc.get("目录名称", ""))

    # 触发治理审计
    audit_rows = store.rows("govern_audit")
    audit_entry = None
    for arow in audit_rows:
        if arow.get("status") == "待审计":
            arow["status"] = "已通过"
            arow["pending"] = False
            arow["审计对象"] = catalog_name
            arow["检查结果"] = "通过"
            arow["审计时间"] = datetime.datetime.now().isoformat()
            arow["整改状态"] = "无需整改"
            arow["triggered_by"] = f"feat_catalog/{index_no}"
            effects.append(FlowEffect(
                "发起审计", "govern_audit", int(arow.get("id", 0)),
                f"目录 {index_no}({catalog_name}) 发布后审计通过，触发特征报告生成",
            ))
            audit_entry = arow
            break

    if audit_entry is None:
        # 创建隐式审计记录
        audit_entry = {"id": 999, "检查结果": "通过"}

    # 触发特征报告生成
    _report_generate(catalog_name, index_no, effects)

    return effects


def _report_generate(catalog_name: str, index_no: str, effects: list[FlowEffect]):
    """生成特征报告。"""
    report_rows = store.rows("report_task")
    for rrow in report_rows:
        if rrow.get("status") == "草稿":
            rrow["status"] = "已生成"
            rrow["pending"] = False
            rrow["特征范围"] = catalog_name
            rrow["最近生成"] = datetime.datetime.now().isoformat()
            rrow["triggered_by"] = f"feat_catalog/{index_no}"
            effects.append(FlowEffect(
                "启动生成", "report_task", int(rrow.get("id", 0)),
                f"审计 {catalog_name} 通过后生成特征报告 {rrow.get('报告编号','')}",
            ))
            break


# ============================ 链路9: 质量规则→特征监控→特征报告 ============================


@_register(
    "quality_rule/确认阻断 → feature_mon/确认漂移 → report_task/启动生成",
    desc="质量规则阻断后联动特征监控漂移告警并生成异常报告。",
)
def flow_quality_to_mon_to_report(quality_rule_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    qr = store.find("quality_rule", quality_rule_id)
    if qr is None or qr.get("status") != "已阻断":
        return effects

    rule_no = str(qr.get("规则编号", ""))
    target_feat = str(qr.get("目标特征", ""))

    # 联动特征监控确认漂移
    mon_rows = store.rows("feature_mon")
    for mrow in mon_rows:
        if target_feat in str(mrow.get("目标特征", "")) and mrow.get("status") == "监控中":
            mrow["status"] = "分布漂移"
            mrow["pending"] = True
            mrow["漂移方向"] = "正向偏移(质量阻断触发)"
            mrow["triggered_by"] = f"quality_rule/{rule_no}"
            effects.append(FlowEffect(
                "确认漂移", "feature_mon", int(mrow.get("id", 0)),
                f"质量规则 {rule_no} 阻断后联动监控 {mrow.get('规则编号','')} 标记分布漂移",
            ))
            break

    # 生成异常报告
    report_rows = store.rows("report_task")
    for rrow in report_rows:
        if rrow.get("status") == "草稿":
            rrow["status"] = "已生成"
            rrow["pending"] = False
            rrow["报告类型"] = "质量异常报告"
            rrow["特征范围"] = target_feat
            rrow["最近生成"] = datetime.datetime.now().isoformat()
            rrow["triggered_by"] = f"quality_rule/{rule_no}"
            effects.append(FlowEffect(
                "启动生成", "report_task", int(rrow.get("id", 0)),
                f"质量规则 {rule_no} 阻断后生成异常报告 {rrow.get('报告编号','')}",
            ))
            break

    return effects


# ============================ 链路10: 调度编排→批量接入/回溯 ============================


@_register(
    "schedule_job/激活计划 → batch_load/启动接入 → backfill_job/启动回溯",
    desc="调度计划激活后按编排顺序触发批量接入和特征回溯。",
)
def flow_schedule_to_batch_to_backfill(schedule_job_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []

    sj = store.find("schedule_job", schedule_job_id)
    if sj is None or sj.get("status") != "执行中":
        return effects

    plan_no = str(sj.get("计划编号", ""))
    task_type = str(sj.get("任务类型", ""))

    # 触发批量接入
    batch_rows = store.rows("batch_load")
    for brow in batch_rows:
        if brow.get("status") == "待执行":
            brow["status"] = "已完成"
            brow["pending"] = False
            brow["接入行数"] = "1250000"
            brow["拒绝行数"] = "350"
            brow["接入时间"] = datetime.datetime.now().isoformat()
            brow["triggered_by"] = f"schedule_job/{plan_no}"
            effects.append(FlowEffect(
                "启动接入", "batch_load", int(brow.get("id", 0)),
                f"调度 {plan_no}({task_type}) 激活后执行批量接入 {brow.get('任务编号','')}，接入125万行",
            ))
            break

    # 触发特征回溯
    backfill_rows = store.rows("backfill_job")
    for bfrow in backfill_rows:
        if bfrow.get("status") == "待执行":
            bfrow["status"] = "已完成"
            bfrow["pending"] = False
            bfrow["当前进度"] = "100%"
            bfrow["数据行数"] = "890000行"
            bfrow["triggered_by"] = f"schedule_job/{plan_no}"
            effects.append(FlowEffect(
                "启动回溯", "backfill_job", int(bfrow.get("id", 0)),
                f"调度 {plan_no}({task_type}) 激活后串联回溯任务 {bfrow.get('任务编号','')}，产出89万行",
            ))
            break

    return effects


# ---------------------------------------------------------------------------
# 链路路由表：根据 (action_module, action_name, status) 找到下游链路
# ---------------------------------------------------------------------------

# key = f"{module}/{action_name}" → [/→/→ 效果收集]
FLOW_TRIGGERS: dict[str, list[Callable[[int], list[FlowEffect]]]] = {}

for link in FLOW_LINKS:
    path = str(link["path"])
    trigger = link["trigger"]
    # first segment is the trigger origin: module/action
    parts = [p.strip() for p in path.split("→")]
    if parts:
        origin = parts[0]  # e.g. "feature_reg/审核注册"
        FLOW_TRIGGERS.setdefault(origin, []).append(trigger)


# ---------------------------------------------------------------------------
# FlowEngine: 注册到路由后的统一入口
# ---------------------------------------------------------------------------


class FlowEngine:
    """跨模块数据流编排引擎。"""

    def on_action(self, module: str, action: str, entry_id: int) -> dict[str, Any]:
        """在业务动作路由执行后调用，触发下游链路消费。

        Returns:
            {"chains": [...链名称], "effects": [...副作用记录], "hits": N}
        """
        triggers = FLOW_TRIGGERS.get(f"{module}/{action}", [])
        all_effects: list[FlowEffect] = []
        chain_hits: list[str] = []

        for trigger_fn in triggers:
            found = False
            for link in FLOW_LINKS:
                if link["trigger"] is trigger_fn and not found:
                    chain_hits.append(str(link["desc"]))
                    found = True
            effects = trigger_fn(entry_id)
            all_effects.extend(effects)

        return {
            "chains": chain_hits,
            "effects": [
                {"name": e.name, "module": e.module, "entry_id": e.entry_id,
                 "description": e.description, "timestamp": e.timestamp}
                for e in all_effects
            ],
            "hits": len(all_effects),
        }

    def replay_link(self, path_key: str, entry_id: int) -> list[FlowEffect]:
        """手动重放指定链路。path_key 如 "derive_rule/启效规则"."""
        triggers = FLOW_TRIGGERS.get(path_key, [])
        effects: list[FlowEffect] = []
        for fn in triggers:
            effects.extend(fn(entry_id))
        return effects


flow_engine = FlowEngine()