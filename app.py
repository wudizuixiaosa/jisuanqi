import streamlit as st
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import akshare as ak


# ==========================================
# 0. 自动获取 LPR 利率数据
# ==========================================
@st.cache_data(ttl=86400, show_spinner="正在同步央行最新 LPR 报价数据...")
def fetch_lpr_data():
    try:
        df = ak.macro_china_lpr()
        df['TRADE_DATE'] = pd.to_datetime(df['TRADE_DATE'])
        df = df.sort_values(by='TRADE_DATE').reset_index(drop=True)
        return df
    except Exception:
        return None


def get_lpr_at_date(lpr_df, target_date, term):
    if lpr_df is None or lpr_df.empty: return None
    target_ts = pd.Timestamp(target_date)
    past_records = lpr_df[lpr_df['TRADE_DATE'] <= target_ts]
    if past_records.empty: return None
    latest_record = past_records.iloc[-1]
    return latest_record['LPR1Y'] if "1Y" in term else latest_record['LPR5Y']


def calculate_repricing_date(loan_start_date, current_record_date, logic):
    if logic == "每年1月1日更新":
        return date(current_record_date.year, 1, 1)
    else:
        try:
            this_year_anniversary = date(current_record_date.year, loan_start_date.month, loan_start_date.day)
        except ValueError:
            this_year_anniversary = date(current_record_date.year, loan_start_date.month, loan_start_date.day - 1)

        if current_record_date >= this_year_anniversary:
            return this_year_anniversary
        else:
            try:
                return date(current_record_date.year - 1, loan_start_date.month, loan_start_date.day)
            except ValueError:
                return date(current_record_date.year - 1, loan_start_date.month, loan_start_date.day - 1)


# 【核心升级】展示正常利率
def calculate_interest_segments(start_date, end_date, loan_start_date, logic, lpr_term, float_rate, prin_base, int_base,
                                lpr_df):
    if start_date >= end_date:
        return {'total_interest': 0.0, 'penalty_on_prin': 0.0, 'compound_on_int': 0.0,
                'normal_daily': 0.0, 'penalty_daily': 0.0, 'rate_str': "-"}

    split_dates = []
    if logic == "每年1月1日更新":
        for y in range(start_date.year, end_date.year + 1):
            rd = date(y, 1, 1)
            if start_date < rd < end_date:
                split_dates.append(rd)
    else:
        for y in range(start_date.year - 1, end_date.year + 2):
            try:
                rd = date(y, loan_start_date.month, loan_start_date.day)
            except ValueError:
                rd = date(y, loan_start_date.month, loan_start_date.day - 1)
            if start_date < rd < end_date:
                split_dates.append(rd)

    split_dates = sorted(list(set(split_dates)))
    intervals = []
    curr = start_date
    for sd in split_dates:
        intervals.append((curr, sd))
        curr = sd
    intervals.append((curr, end_date))

    total_penalty_prin = 0.0
    total_compound_int = 0.0
    total_interest = 0.0
    exec_rate_strs = []
    last_normal_daily = 0.0
    last_penalty_daily = 0.0

    for s_dt, e_dt in intervals:
        days = (e_dt - s_dt).days
        if days <= 0: continue

        rd = calculate_repricing_date(loan_start_date, s_dt, logic)
        lpr_val = get_lpr_at_date(lpr_df, rd, lpr_term)
        lpr_val = float(lpr_val) if lpr_val else 3.45

        normal_rate = lpr_val + float_rate
        penalty_rate = normal_rate * 1.5

        daily_normal_rate = normal_rate / 100.0 / 360.0
        daily_penalty_rate = penalty_rate / 100.0 / 360.0

        last_normal_daily = prin_base * daily_normal_rate
        last_penalty_daily = prin_base * daily_penalty_rate

        seg_penalty_prin = last_penalty_daily * days
        seg_compound_int = int_base * daily_penalty_rate * days

        total_penalty_prin += seg_penalty_prin
        total_compound_int += seg_compound_int
        total_interest += (seg_penalty_prin + seg_compound_int)

        # 修正：前端显示正常的实际执行利率，不再显示1.5倍后的罚息利率
        rate_str = f"{normal_rate:.4f}%"
        if rate_str not in exec_rate_strs:
            exec_rate_strs.append(rate_str)

    return {
        'total_interest': total_interest,
        'penalty_on_prin': total_penalty_prin,
        'compound_on_int': total_compound_int,
        'normal_daily': last_normal_daily,
        'penalty_daily': last_penalty_daily,
        'rate_str': " | ".join(exec_rate_strs)
    }


# ==========================================
# 1. 核心批处理计算引擎 (事件流架构 + 瀑布流抵扣)
# ==========================================
def generate_full_ledger(init_params, repayments, target_date, lpr_df):
    records = []
    loan_start = init_params['loan_date']
    base_date = init_params['overdue_date']

    bal = init_params['init_bal']
    prin = init_params['init_prin']

    # 建立利息的独立记账桶（为了算清楚最后的摘要）
    unpaid_base_int = init_params['init_int']  # 初始欠息
    unpaid_penalty = 0.0  # 已结罚息
    unpaid_compound = 0.0  # 已结复利

    unsettled_penalty = 0.0  # 本期暂存罚息
    unsettled_compound = 0.0  # 本期暂存复利

    records.append({
        "日期": base_date.strftime("%Y-%m-%d"),
        "说明": "初始逾期台账",
        "实际执行利率": f"{init_params['overdue_rate']:.4f}%",
        "贷款余额": float(bal),
        "利息计算天数": 0,
        "本次已还本金": 0.0,
        "本次已还利息": 0.0,
        "本次未还本金": 0.0,
        "本次未还利息": 0.0,
        "积欠本金合计": float(prin),
        "积欠利息合计": float(unpaid_base_int),
        "日利息": float(prin * (init_params['overdue_rate']) / 100.0 / 360.0),
        "罚息日利息": float(prin * init_params['overdue_rate'] * 1.5 / 100.0 / 360.0),
        "罚息利息": 0.0,
        "复利": 0.0,
        "罚息": 0.0
    })

    if target_date <= base_date:
        return pd.DataFrame(records), None

    events = {}

    def init_event(d):
        if d not in events:
            events[d] = {'repay_p': 0.0, 'repay_i': 0.0, 'is_settle': False, 'is_target': False}

    settle_day = base_date.day
    curr_m = base_date.replace(day=1)
    while curr_m <= target_date.replace(day=1) + relativedelta(months=1):
        try:
            sd = curr_m.replace(day=settle_day)
        except ValueError:
            sd = curr_m + relativedelta(day=31)
        if base_date < sd <= target_date:
            init_event(sd)
            events[sd]['is_settle'] = True
        curr_m += relativedelta(months=1)

    for r in repayments:
        r_date = r['date']
        if base_date < r_date <= target_date:
            init_event(r_date)
            events[r_date]['repay_p'] += r['p']
            events[r_date]['repay_i'] += r['i']

    init_event(target_date)
    events[target_date]['is_target'] = True

    sorted_dates = sorted(events.keys())
    curr_date = base_date

    for d in sorted_dates:
        ev = events[d]
        days = (d - curr_date).days

        # 计息基数 = 初始欠息 + 已结罚息 + 已结复利 (注意暂存区不生息)
        current_settled_int = unpaid_base_int + unpaid_penalty + unpaid_compound

        if days > 0:
            res = calculate_interest_segments(
                curr_date, d, loan_start, init_params['logic'],
                init_params['term'], init_params['float_rate'], prin, current_settled_int, lpr_df)

            unsettled_penalty += res['penalty_on_prin']
            unsettled_compound += res['compound_on_int']
            amt = res['total_interest']
        else:
            res = {'total_interest': 0.0, 'penalty_on_prin': 0.0, 'compound_on_int': 0.0,
                   'normal_daily': 0.0, 'penalty_daily': 0.0, 'rate_str': "-"}
            amt = 0.0

        # 扣款处理
        rp, ri = ev['repay_p'], ev['repay_i']
        bal -= rp
        prin -= rp

        # 【瀑布流抵扣算法】还款冲销顺序：暂存复利->暂存罚息->已结复利->已结罚息->本金欠息
        if ri > 0:
            ri_rem = ri

            # 1. 抵扣本期暂存区
            pay_u_comp = min(ri_rem, unsettled_compound)
            unsettled_compound -= pay_u_comp
            ri_rem -= pay_u_comp

            pay_u_pen = min(ri_rem, unsettled_penalty)
            unsettled_penalty -= pay_u_pen
            ri_rem -= pay_u_pen

            # 2. 抵扣已结滚息区
            pay_comp = min(ri_rem, unpaid_compound)
            unpaid_compound -= pay_comp
            ri_rem -= pay_comp

            pay_pen = min(ri_rem, unpaid_penalty)
            unpaid_penalty -= pay_pen
            ri_rem -= pay_pen

            # 3. 抵扣初始底层欠息
            pay_base = min(ri_rem, unpaid_base_int)
            unpaid_base_int -= pay_base
            ri_rem -= pay_base

        is_settle = ev['is_settle']
        has_repay = (rp > 0 or ri > 0)
        is_tgt = ev['is_target']

        if is_settle:
            label = "自动结息"
            if has_repay: label = "自动结息+还款"
            if is_tgt: label = "查询日(逢结息)"
        else:
            if has_repay:
                label = "中途还款"
                if is_tgt: label = "查询日(含还款)"
            else:
                label = "最终查询日"

        row_unpaid_int = max(0.0, amt - ri)

        # 页面展示的总利息 = 所有池子剩余加总
        display_total_int = (unpaid_base_int + unpaid_penalty + unpaid_compound) + (
                    unsettled_penalty + unsettled_compound)

        records.append({
            "日期": d.strftime("%Y-%m-%d"),
            "说明": label,
            "实际执行利率": res['rate_str'] if days > 0 else "-",
            "贷款余额": float(bal),
            "利息计算天数": int(days),
            "本次已还本金": float(rp),
            "本次已还利息": float(ri),
            "本次未还本金": 0.0,
            "本次未还利息": float(row_unpaid_int),
            "积欠本金合计": float(prin),
            "积欠利息合计": float(display_total_int),
            "日利息": float(res['normal_daily']),
            "罚息日利息": float(res['penalty_daily']),
            "罚息利息": float(res['penalty_on_prin']),
            "复利": float(res['compound_on_int']),
            "罚息": float(res['total_interest'])
        })

        # 结息日动作：把暂存池倒入已结池（产生利滚利）
        if is_settle:
            unpaid_penalty += unsettled_penalty
            unpaid_compound += unsettled_compound
            unsettled_penalty = 0.0
            unsettled_compound = 0.0

        curr_date = d

    # 4. 生成底部的摘要字典
    final_base_int = unpaid_base_int
    final_penalty = unpaid_penalty + unsettled_penalty
    final_compound = unpaid_compound + unsettled_compound
    final_total_int = final_base_int + final_penalty + final_compound

    summary = {
        'prin': prin,
        'base_int': final_base_int,
        'penalty': final_penalty,
        'compound': final_compound,
        'total_int': final_total_int,
        'total_pi': prin + final_total_int
    }

    return pd.DataFrame(records), summary


# ==========================================
# 2. 前台网页界面
# ==========================================
st.set_page_config(page_title="LPR自动分段计息器", layout="wide")
st.title("⚖️ 逾期罚复息精准自动分段计算器 (专业标准版)")

lpr_df = fetch_lpr_data()

if 'init_params' not in st.session_state: st.session_state.init_params = None
if 'repayments' not in st.session_state: st.session_state.repayments = []
if 'final_result' not in st.session_state: st.session_state.final_result = None

col1, col2 = st.columns([1.5, 3.5])

with col1:
    st.header("📋 1. 合同及放款")
    with st.expander("设置贷款基准与重定价逻辑", expanded=(st.session_state.init_params is None)):
        loan_date = st.date_input("放款日期", value=date(2022, 1, 24))
        original_loan_amount = st.number_input("放款总额", value=140000.0, step=10000.0)
        logic = st.radio("重定价政策", ["按年更新 (发放日对月对日)", "每年1月1日更新"])
        lpr_term = st.radio("LPR 品种", ["1年期 (1Y)", "5年期以上 (5Y)"], horizontal=True)
        lpr_float_rate = st.number_input("LPR加减点 (%)", value=2.45, step=0.01)

        st.divider()
        init_overdue_date = st.date_input("逾期起始日", value=date(2023, 11, 21))
        init_balance = st.number_input("逾期时贷款余额", value=140000.0)
        init_prin = st.number_input("逾期积欠本金", value=140000.0)
        init_int = st.number_input("逾期积欠利息", value=11861.19)

        if st.button("🚀 锁定合同并初始化", type="primary", use_container_width=True):
            issue_lpr = float(get_lpr_at_date(lpr_df, loan_date, lpr_term) or 3.45)
            repricing_day_overdue = calculate_repricing_date(loan_date, init_overdue_date, logic)
            overdue_lpr = float(get_lpr_at_date(lpr_df, repricing_day_overdue, lpr_term) or 3.45)

            st.session_state.init_params = {
                'loan_date': loan_date, 'amount': original_loan_amount,
                'logic': logic, 'term': lpr_term, 'float_rate': lpr_float_rate,
                'overdue_date': init_overdue_date, 'overdue_rate': overdue_lpr + lpr_float_rate,
                'init_bal': init_balance, 'init_prin': init_prin, 'init_int': init_int
            }
            st.session_state.repayments = []
            st.session_state.final_result = None
            st.rerun()

    if st.session_state.init_params:
        st.header("➕ 2. 纯粹录入期间还款")
        op_date = st.date_input("发生还款的日期", value=date(2024, 1, 10))
        c_p, c_i = st.columns(2)
        re_prin = c_p.number_input("偿还本金 (元)", value=0.0, step=100.0)
        re_int = c_i.number_input("偿还利息 (元)", value=0.0, step=100.0)

        if st.button("✅ 记入这笔还款", use_container_width=True):
            if op_date > st.session_state.init_params['overdue_date']:
                st.session_state.repayments.append({'date': op_date, 'p': re_prin, 'i': re_int})
                st.rerun()
            else:
                st.error("还款日期必须晚于初始逾期日！")

        if len(st.session_state.repayments) > 0:
            st.write("📝 **已录入的还款池：**")
            rep_df = pd.DataFrame(st.session_state.repayments)
            rep_df.columns = ["日期", "还本金", "还利息"]
            st.dataframe(rep_df, use_container_width=True, hide_index=True)
            if st.button("🗑️ 撤销最后一条还款", use_container_width=True):
                st.session_state.repayments.pop()
                st.rerun()

        st.header("🚀 3. 一键生成终极台账")
        target_date = st.date_input("最终要计算到哪一天？", value=date(2025, 1, 3))

        if st.button("⚡ 融合所有数据，一键推演生成！", type="primary", use_container_width=True):
            if target_date <= st.session_state.init_params['overdue_date']:
                st.error("目标日期必须晚于初始逾期日！")
            else:
                df, summary = generate_full_ledger(
                    st.session_state.init_params,
                    st.session_state.repayments,
                    target_date,
                    lpr_df
                )
                st.session_state.final_result = {'df': df, 'summary': summary}
                st.rerun()

with col2:
    st.header("📊 4. 银行级精细对账单")
    if st.session_state.final_result is not None:
        df = st.session_state.final_result['df']
        summary = st.session_state.final_result['summary']

        numeric_cols = [
            "贷款余额", "本次已还本金", "本次已还利息", "本次未还本金", "本次未还利息",
            "积欠本金合计", "积欠利息合计", "日利息", "罚息日利息", "罚息利息", "复利", "罚息"
        ]
        col_config = {col: st.column_config.NumberColumn(format="%.2f") for col in numeric_cols}

        st.dataframe(
            df,
            use_container_width=True, height=600, hide_index=True,
            column_config=col_config
        )

        # 构建注脚文本
        footer_text = (f"注：贷款本息 {summary['total_pi']:.2f} 元，"
                       f"其中：贷款本金 {summary['prin']:.2f} 元，积欠利息 {summary['total_int']:.2f} 元，"
                       f"（其中：欠息 {summary['base_int']:.2f} 元，罚息 {summary['penalty']:.2f} 元，复利 {summary['compound']:.2f} 元）。")

        st.info(f"💡 **对账汇总：**\n\n{footer_text}")

        # 组装下载用的 CSV：对 DataFrame 强制保留两位小数，并在末尾追加注脚行
        export_df = df.round(2).copy()
        csv_data = export_df.to_csv(index=False)
        final_csv_content = csv_data.encode('utf-8-sig') + f"\n{footer_text}\n".encode('utf-8-sig')

        st.download_button("📥 导出报表 (含底注)", final_csv_content, "批处理计息明细.csv", "text/csv")

    elif st.session_state.init_params is not None:
        st.info("👈 请在左侧配置完成后，点击【⚡ 融合所有数据，一键推演生成！】以查看台账。")
