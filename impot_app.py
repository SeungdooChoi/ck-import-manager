import streamlit as st
import pandas as pd
from sqlalchemy import text
from datetime import datetime
import time
import pytz
import re

# ==========================================
# 0. 기본 설정 및 스타일
# ==========================================
st.set_page_config(page_title="수입진행관리 (CK Global)", layout="wide")
KST = pytz.timezone('Asia/Seoul')

def get_kst_today():
    return datetime.now(KST).date()

st.markdown("""
<style>
    .block-container { max-width: 1400px; padding-top: 1rem; }
    .status-pending { color: #f59f00; font-weight: bold; }
    .status-arrived { color: #0ca678; font-weight: bold; }
    .status-canceled { color: #fa5252; font-weight: bold; }
    div[data-testid="stExpander"] { border: 1px solid #dee2e6; border-radius: 4px; }
    
    /* 테이블 스타일 */
    .dataframe { font-size: 12px !important; }
    
    .metric-box {
        background-color: #f1f3f5;
        border: 1px solid #dee2e6;
        padding: 15px;
        border-radius: 8px;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

# DB 연결 및 스키마 업데이트
try:
    conn = st.connection("supabase", type="sql")
    with conn.session as s:
        # ck_code 컬럼 추가 (관리용 임의 코드)
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS ck_code TEXT;"))
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS size TEXT;"))
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS unit_price NUMERIC;"))
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS supplier TEXT;"))
        s.commit()
except Exception as e:
    st.error(f"DB 연결 오류: .streamlit/secrets.toml을 확인하세요.\n{e}")
    st.stop()

# ==========================================
# 1. 데이터 조회 및 액션 함수
# ==========================================

def get_products_df():
    """DB에 등록된 품목 리스트 조회"""
    with conn.session as s:
        df = pd.DataFrame(s.execute(text("SELECT product_id, product_name, product_code, category, unit FROM products WHERE is_active = TRUE ORDER BY category, product_name")).fetchall())
        if not df.empty:
            df.columns = ['ID', '품목명', '품목코드', '카테고리', '단위']
        return df

def register_new_product(code, name, cat, unit):
    """신규 품목 DB 등록"""
    try:
        with conn.session as s:
            # 중복 체크
            chk = s.execute(text("SELECT 1 FROM products WHERE product_code = :code"), {"code": code}).fetchone()
            if chk: return False, "이미 존재하는 품목코드입니다."
            
            s.execute(text("""
                INSERT INTO products (product_code, product_name, category, unit, is_active)
                VALUES (:code, :name, :cat, :unit, TRUE)
            """), {"code": code, "name": name, "cat": cat, "unit": unit})
            s.commit()
        return True, "품목 등록 완료"
    except Exception as e: return False, str(e)

def get_schedules_detailed(status_filter='ALL'):
    """일정 상세 조회 (조인 포함)"""
    with conn.session as s:
        base_sql = """
            SELECT s.id, s.expected_date, s.supplier, p.product_name, s.ck_code, s.size, 
                   s.unit_price, s.quantity, p.unit, s.status, s.note, p.product_code as db_prod_code
            FROM import_schedules s
            JOIN products p ON s.product_id = p.product_id
        """
        if status_filter != 'ALL':
            base_sql += f" WHERE s.status = '{status_filter}'"
        
        base_sql += " ORDER BY s.expected_date ASC, s.supplier ASC"
        
        df = pd.DataFrame(s.execute(text(base_sql)).fetchall())
        if not df.empty:
            df.columns = ['ID', '입항일', '공급사', '품목명', 'CK코드', '사이즈', '단가', '수량', '단위', '상태', '비고', '품목코드(DB)']
        return df

def add_schedule(pid, date, qty, note, size=None, supplier=None, price=0, ck_code=None):
    try:
        with conn.session as s:
            s.execute(text("""
                INSERT INTO import_schedules (product_id, expected_date, quantity, note, status, size, supplier, unit_price, ck_code)
                VALUES (:pid, :date, :qty, :note, 'PENDING', :size, :supp, :price, :ck)
            """), {"pid": pid, "date": date, "qty": qty, "note": note, "size": size, "supp": supplier, "price": price, "ck": ck_code})
            s.commit()
        return True, "등록 완료"
    except Exception as e:
        return False, str(e)

def update_schedule_status(sid, new_status):
    try:
        with conn.session as s:
            s.execute(text("UPDATE import_schedules SET status = :st WHERE id = :sid"), {"st": new_status, "sid": sid})
            s.commit()
        return True, "상태 변경 완료"
    except Exception as e: return False, str(e)

def delete_schedule(sid):
    try:
        with conn.session as s:
            s.execute(text("DELETE FROM import_schedules WHERE id = :sid"), {"sid": sid})
            s.commit()
        return True, "삭제 완료"
    except Exception as e: return False, str(e)

# ==========================================
# 2. 엑셀 파싱 로직 (업데이트됨)
# ==========================================
def parse_excel_and_validate(df):
    """
    엑셀 데이터를 파싱하고, DB에 품목이 존재하는지 검증합니다.
    - 품목이 DB에 없으면 'error' 리스트에 담아 반환 (자동등록 X)
    - CK 코드는 엑셀의 값을 그대로 사용
    """
    valid_data = []
    errors = []
    
    # DB 품목 맵핑 (품명 -> ID)
    p_df = get_products_df()
    if p_df.empty:
        return [], ["시스템에 등록된 품목이 없습니다. 품목 관리 탭에서 먼저 품목을 등록해주세요."]
    
    # 공백 제거 및 소문자 변환하여 매칭 확률 높임
    product_map = {row['품목명'].replace(" ", "").lower(): row['ID'] for _, row in p_df.iterrows()}
    
    # 헤더 찾기 (CK, 품명)
    header_row_idx = -1
    for i, row in df.iterrows():
        row_str = row.astype(str).str.cat()
        if 'CK' in row_str and '품명' in row_str:
            header_row_idx = i
            break
            
    if header_row_idx == -1:
        return [], ["헤더('CK', '품명')를 찾을 수 없습니다."]

    df.columns = df.iloc[header_row_idx]
    data_df = df.iloc[header_row_idx+1:].reset_index(drop=True)
    
    cols = data_df.columns.astype(str)
    # 컬럼 매핑
    col_map = {
        'ck': next((c for c in cols if 'CK' in c), None),
        'name': next((c for c in cols if '품명' in c), None),
        'size': next((c for c in cols if '사이즈' in c), None),
        'price': next((c for c in cols if '단가' in c), None),
        'date': next((c for c in cols if '입항' in c or 'ETA' in c), None)
    }
    
    # 수량 컬럼 찾기 (단가 옆)
    try:
        price_col_idx = list(cols).index(col_map['price'])
        col_map['qty'] = cols[price_col_idx + 1]
    except: col_map['qty'] = None

    current_supplier = ""
    
    for idx, row in data_df.iterrows():
        # 공급사 (A열 추정)
        raw_supp = str(row.iloc[0]).strip()
        if raw_supp and raw_supp.lower() != 'nan': current_supplier = raw_supp
        
        ck_val = str(row[col_map['ck']]).strip()
        name_val = str(row[col_map['name']]).strip()
        
        # 유효 데이터 확인
        if (not ck_val or ck_val == 'nan') and (not name_val or name_val == 'nan'): continue
        
        # 품목 매칭 확인
        search_key = name_val.replace(" ", "").lower()
        pid = product_map.get(search_key)
        
        if not pid:
            errors.append(f"[행 {idx+header_row_idx+2}] 알 수 없는 품목: '{name_val}' (CK: {ck_val}) - 품목 관리 탭에서 먼저 등록해주세요.")
            continue
            
        # 데이터 파싱
        try:
            # 날짜
            raw_date = row[col_map['date']]
            eta = get_kst_today()
            if pd.notna(raw_date):
                if isinstance(raw_date, str):
                    eta = datetime.strptime(raw_date, "%y/%m/%d").date()
                    if eta.year < 2000: eta = eta.replace(year=eta.year+2000)
                else: eta = pd.to_datetime(raw_date).date()
            
            # 수량/단가
            qty = 0
            try: qty = int(float(str(row[col_map['qty']]).replace(',', '')))
            except: pass
            
            price = 0
            try: price = float(str(row[col_map['price']]).replace(',', ''))
            except: pass
            
            if qty == 0: continue
            
            size_val = str(row[col_map['size']]) if col_map['size'] else ""
            if size_val == 'nan': size_val = ""

            valid_data.append({
                'pid': pid, 'date': eta, 'qty': qty, 'note': f"엑셀등록({current_supplier})",
                'size': size_val, 'supplier': current_supplier, 'price': price, 'ck_code': ck_val
            })
            
        except Exception as e:
            errors.append(f"[행 {idx+header_row_idx+2}] 데이터 파싱 오류: {str(e)}")
            
    return valid_data, errors

# ==========================================
# 3. 메인 UI 구성
# ==========================================

st.title("🚢 수입 관리 시스템")

tab_status, tab_manage, tab_product = st.tabs(["📊 수입진행상황 (현황판)", "📝 일정 등록/관리", "📦 품목 관리 (DB)"])

# --- TAB 1: 수입진행상황 (엑셀 뷰) ---
with tab_status:
    st.markdown("### 📅 수입 일정 현황")
    
    col_f1, col_f2 = st.columns([1, 4])
    with col_f1:
        view_opt = st.radio("조회 상태", ["전체", "진행중 (PENDING)", "입고완료 (ARRIVED)"], index=1)
        
    status_filter = 'ALL'
    if "진행중" in view_opt: status_filter = 'PENDING'
    elif "입고완료" in view_opt: status_filter = 'ARRIVED'
    
    df = get_schedules_detailed(status_filter)
    
    if df.empty:
        st.info("조회된 데이터가 없습니다.")
    else:
        # 요약 지표
        total_qty = df['수량'].sum()
        total_count = len(df)
        
        c1, c2, c3 = st.columns(3)
        c1.markdown(f"<div class='metric-box'>총 건수<br><b>{total_count}</b> 건</div>", unsafe_allow_html=True)
        c2.markdown(f"<div class='metric-box'>총 수량<br><b>{total_qty:,}</b></div>", unsafe_allow_html=True)
        c3.markdown(f"<div class='metric-box'>조회 기준<br><b>{view_opt}</b></div>", unsafe_allow_html=True)
        st.markdown("")

        # 테이블 뷰 (엑셀 스타일)
        # 컬럼 순서 재배치
        display_df = df[['입항일', '공급사', '품목명', 'CK코드', '사이즈', '단가', '수량', '단위', '상태', '비고']].copy()
        
        # 스타일링을 위한 포맷팅
        st.dataframe(
            display_df,
            column_config={
                "입항일": st.column_config.DateColumn("ETA", format="YYYY-MM-DD"),
                "단가": st.column_config.NumberColumn("단가($)", format="$%.2f"),
                "수량": st.column_config.NumberColumn("수량", format="%d"),
            },
            use_container_width=True,
            height=600,
            hide_index=True
        )

# --- TAB 2: 일정 등록/관리 ---
with tab_manage:
    sub_t1, sub_t2, sub_t3 = st.tabs(["✍️ 수기 등록", "📂 엑셀 일괄 등록", "🛠️ 등록 건 관리"])
    
    # 1. 수기 등록
    with sub_t1:
        st.markdown("##### 신규 수입 일정 등록")
        st.caption("※ 시스템에 등록된 품목만 선택 가능합니다. 없는 품목은 '품목 관리' 탭에서 등록하세요.")
        
        p_df = get_products_df()
        if p_df.empty:
            st.error("등록된 품목이 없습니다.")
        else:
            # Selectbox용 옵션 생성
            p_opts = {row['ID']: f"[{row['카테고리']}] {row['품목명']} (DB코드:{row['품목코드']})" for _, row in p_df.iterrows()}
            
            with st.form("manual_add"):
                c1, c2 = st.columns(2)
                sel_pid = c1.selectbox("품목 선택", options=p_opts.keys(), format_func=lambda x: p_opts[x])
                ck_code_in = c2.text_input("CK 코드 (관리용, 임의입력)", placeholder="예: CK-2501")
                
                c3, c4, c5 = st.columns(3)
                supp_in = c3.text_input("공급사")
                date_in = c4.date_input("입항 예정일", value=get_kst_today())
                size_in = c5.text_input("사이즈/규격")
                
                c6, c7 = st.columns(2)
                qty_in = c6.number_input("수량", min_value=1)
                price_in = c7.number_input("단가 ($)", min_value=0.0, step=0.1)
                
                note_in = st.text_area("비고")
                
                if st.form_submit_button("일정 등록", type="primary"):
                    succ, msg = add_schedule(sel_pid, date_in, qty_in, note_in, size_in, supp_in, price_in, ck_code_in)
                    if succ:
                        st.success("등록되었습니다!")
                        time.sleep(1)
                        st.rerun()
                    else: st.error(f"실패: {msg}")

    # 2. 엑셀 일괄 등록
    with sub_t2:
        st.markdown("##### 엑셀 파일 업로드")
        st.caption("※ 엑셀의 '품명'이 시스템의 '품목명'과 일치해야 등록됩니다. (불일치 시 에러 목록 표시)")
        
        up_file = st.file_uploader("수입진행상황 엑셀 파일", type=['xlsx', 'csv'])
        if up_file:
            if st.button("파일 분석 및 등록"):
                try:
                    df_up = pd.read_csv(up_file) if up_file.name.endswith('.csv') else pd.read_excel(up_file)
                    valid_rows, err_list = parse_excel_and_validate(df_up)
                    
                    if err_list:
                        st.error(f"{len(err_list)}건의 문제가 발견되었습니다.")
                        with st.expander("에러 상세 보기 (등록되지 않음)", expanded=True):
                            for e in err_list: st.write(f"- {e}")
                            
                    if valid_rows:
                        st.success(f"{len(valid_rows)}건의 유효한 데이터를 찾았습니다. 등록을 진행합니다.")
                        prog = st.progress(0)
                        cnt = 0
                        for i, d in enumerate(valid_rows):
                            ok, _ = add_schedule(d['pid'], d['date'], d['qty'], d['note'], d['size'], d['supplier'], d['price'], d['ck_code'])
                            if ok: cnt += 1
                            prog.progress((i+1)/len(valid_rows))
                        
                        st.toast(f"{cnt}건 등록 완료!")
                        time.sleep(1)
                        st.rerun()
                    elif not err_list:
                        st.warning("등록할 데이터가 없습니다.")
                        
                except Exception as e:
                    st.error(f"파일 처리 중 오류: {e}")

    # 3. 등록 건 관리
    with sub_t3:
        st.markdown("##### 등록된 일정 관리 (상태 변경/삭제)")
        m_df = get_schedules_detailed('ALL')
        if not m_df.empty:
            for i, row in m_df.iterrows():
                with st.expander(f"{row['입항일']} | {row['품목명']} ({row['CK코드']}) - {row['상태']}"):
                    mc1, mc2 = st.columns([3, 1])
                    with mc1:
                        st.write(f"공급사: {row['공급사']} / 수량: {row['수량']:,} / 단가: ${row['단가']}")
                        st.write(f"비고: {row['비고']}")
                    with mc2:
                        if row['상태'] == 'PENDING':
                            if st.button("도착 처리", key=f"btn_arr_{row['ID']}"):
                                update_schedule_status(row['ID'], 'ARRIVED')
                                st.rerun()
                        else:
                            if st.button("진행중 복구", key=f"btn_pen_{row['ID']}"):
                                update_schedule_status(row['ID'], 'PENDING')
                                st.rerun()
                        
                        if st.button("삭제", key=f"btn_del_{row['ID']}", type="primary"):
                            delete_schedule(row['ID'])
                            st.rerun()
        else:
            st.info("데이터가 없습니다.")

# --- TAB 3: 품목 관리 (DB) ---
with tab_product:
    st.markdown("### 📦 시스템 품목 관리")
    st.caption("이곳에서 등록한 품목은 재고현황표와 수입관리 모두에서 사용됩니다.")
    
    col_p1, col_p2 = st.columns([1, 2])
    
    # 신규 등록
    with col_p1:
        st.markdown("#### 신규 품목 등록")
        with st.form("new_prod_form"):
            new_code = st.text_input("품목코드 (고유값)", placeholder="예: P1001")
            new_name = st.text_input("품목명")
            new_cat = st.text_input("카테고리", placeholder="예: 수입")
            new_unit = st.text_input("기본 단위", value="Box")
            
            if st.form_submit_button("품목 저장", type="primary"):
                if new_code and new_name:
                    succ, msg = register_new_product(new_code, new_name, new_cat, new_unit)
                    if succ:
                        st.success(msg)
                        time.sleep(1)
                        st.rerun()
                    else: st.error(msg)
                else:
                    st.warning("코드와 품목명은 필수입니다.")
    
    # 조회
    with col_p2:
        st.markdown("#### 등록된 품목 리스트")
        curr_prods = get_products_df()
        if not curr_prods.empty:
            st.dataframe(curr_prods, use_container_width=True, hide_index=True)
        else:
            st.info("등록된 품목이 없습니다.")