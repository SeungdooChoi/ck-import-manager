import streamlit as st
import pandas as pd
from sqlalchemy import text
from datetime import datetime

# DB 연결 (기존 앱과 동일한 설정 사용)
st.set_page_config(page_title="수입 진행 관리", layout="wide")
conn = st.connection("supabase", type="sql")

st.title("🚢 수입 진행 관리")

# 1. 신규 수입 일정 등록
with st.expander("➕ 신규 일정 등록", expanded=True):
    # 품목 불러오기
    products_df = pd.DataFrame(conn.query("SELECT product_id, product_name, product_code FROM products"))
    
    if not products_df.empty:
        prod_options = {row['product_id']: f"{row['product_name']} ({row['product_code']})" for i, row in products_df.iterrows()}
        
        with st.form("add_schedule"):
            sel_pid = st.selectbox("품목 선택", options=prod_options.keys(), format_func=lambda x: prod_options[x])
            c1, c2 = st.columns(2)
            e_date = c1.date_input("입고 예정일")
            e_qty = c2.number_input("예정 수량", min_value=1)
            e_note = st.text_input("비고 (선적정보 등)")
            
            if st.form_submit_button("등록"):
                with conn.session as s:
                    s.execute(text("""
                        INSERT INTO import_schedules (product_id, expected_date, quantity, note)
                        VALUES (:pid, :dt, :qty, :nt)
                    """), {"pid": sel_pid, "dt": e_date, "qty": e_qty, "nt": e_note})
                    s.commit()
                st.success("등록되었습니다.")
                st.rerun()

# 2. 진행 현황 조회 및 관리
st.subheader("📋 수입 진행 현황")

# 데이터 조회
df = pd.DataFrame(conn.query("""
    SELECT s.id, p.product_name, p.product_code, s.expected_date, s.quantity, s.status, s.note
    FROM import_schedules s
    JOIN products p ON s.product_id = p.product_id
    ORDER BY s.expected_date ASC
"""))

if not df.empty:
    for idx, row in df.iterrows():
        # 스타일링
        status_color = "#e6fcf5" if row['status'] == 'PENDING' else "#f1f3f5"
        
        with st.container(border=True):
            c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
            c1.markdown(f"**{row['product_name']}** ({row['product_code']})")
            c1.caption(f"비고: {row['note']}")
            
            c2.metric("입고 예정일", str(row['expected_date']))
            c3.metric("수량", f"{row['quantity']:,}")
            
            with c4:
                if row['status'] == 'PENDING':
                    st.info("🚢 이동중")
                    if st.button("도착 완료 처리", key=f"arv_{row['id']}"):
                        with conn.session as s:
                            s.execute(text("UPDATE import_schedules SET status = 'ARRIVED' WHERE id = :id"), {"id": row['id']})
                            s.commit()
                        st.rerun()
                else:
                    st.success("✅ 도착됨")
else:
    st.info("등록된 수입 일정이 없습니다.")