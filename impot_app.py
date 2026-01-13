import streamlit as st
import pandas as pd
from sqlalchemy import text
from datetime import datetime
import time
import pytz
import re
import io
import json

# ==========================================
# 0. 기본 설정 및 스타일
# ==========================================
st.set_page_config(page_title="수입진행관리 (CK Global)", layout="wide")
KST = pytz.timezone('Asia/Seoul')

def get_kst_today():
    return datetime.now(KST).date()

st.markdown("""
<style>
    .block-container { max-width: 98% !important; padding-top: 1rem; }
    
    /* 상태 배지 스타일 */
    .status-badge { padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.8em; }
    .status-pending { background-color: #fff3bf; color: #f08c00; }
    .status-arrived { background-color: #d3f9d8; color: #2b8a3e; }
    .status-canceled { background-color: #ffe3e3; color: #c92a2a; }

    /* 메트릭 박스 스타일 */
    .metric-box {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        padding: 10px;
        border-radius: 8px;
        text-align: center;
        font-size: 14px;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }
    
    /* 폼 헤더 스타일 */
    .form-header {
        font-weight: 700;
        font-size: 1.1em;
        margin-top: 20px;
        margin-bottom: 10px;
        border-bottom: 2px solid #e9ecef;
        padding-bottom: 5px;
        color: #495057;
    }
    
    /* 데이터프레임 스타일 */
    .stDataFrame { font-size: 12px; }
    
    /* 동적 입력 필드 스타일 */
    .dynamic-row {
        background-color: #f8f9fa;
        padding: 10px;
        border-radius: 5px;
        margin-bottom: 5px;
        border: 1px solid #eee;
    }
</style>
""", unsafe_allow_html=True)

# DB 연결 및 스키마 업데이트
try:
    conn = st.connection("supabase", type="sql")
    with conn.session as s:
        # 모든 필요한 컬럼이 존재하는지 확인하고 없으면 추가
        cols_to_add = [
            ("ck_code", "TEXT"), ("size", "TEXT"), ("unit_price", "NUMERIC"), ("supplier", "TEXT"),
            ("global_code", "TEXT"), ("doojin_code", "TEXT"), ("agency", "TEXT"), ("agency_contract", "TEXT"),
            ("origin", "TEXT"), ("packing", "TEXT"), ("open_qty", "NUMERIC"), ("doc_qty", "NUMERIC"),
            ("box_qty", "NUMERIC"), ("unit2", "TEXT"), ("open_amount", "NUMERIC"), ("doc_amount", "NUMERIC"),
            ("tt_check", "TEXT"), ("bank", "TEXT"), ("usance", "TEXT"), ("at_sight", "TEXT"),
            ("open_date", "DATE"), ("lc_no", "TEXT"), ("invoice_no", "TEXT"), ("bl_no", "TEXT"),
            ("lg_no", "TEXT"), ("insurance", "TEXT"), ("customs_broker_date", "DATE"), ("etd", "DATE"),
            ("arrival_date", "DATE"), ("warehouse", "TEXT"), ("actual_in_qty", "NUMERIC"), ("destination", "TEXT"),
            ("doc_acceptance", "DATE"), ("acceptance_rate", "NUMERIC"), ("maturity_date", "DATE"),
            ("ext_maturity_date", "DATE"), ("acceptance_fee", "NUMERIC"), ("discount_fee", "NUMERIC"),
            ("payment_date", "DATE"), ("payment_amount", "NUMERIC"), ("exchange_rate", "NUMERIC"),
            ("balance", "NUMERIC"), ("avg_exchange_rate", "NUMERIC"),
            # 통관 및 신고 정보 (JSONB로 저장하여 N개 데이터 지원)
            ("clearance_info", "JSONB"), ("declaration_info", "JSONB")
        ]
        for col_name, col_type in cols_to_add:
            s.execute(text(f"ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS {col_name} {col_type};"))
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
            chk = s.execute(text("SELECT 1 FROM products WHERE product_code = :code"), {"code": code}).fetchone()
            if chk: return False, "이미 존재하는 품목코드입니다."
            s.execute(text("""
                INSERT INTO products (product_code, product_name, category, unit, is_active)
                VALUES (:code, :name, :cat, :unit, TRUE)
            """), {"code": code, "name": name, "cat": cat, "unit": unit})
            s.commit()
        return True, "품목 등록 완료"
    except Exception as e: return False, str(e)

def get_full_schedule_data(status_filter='ALL'):
    """모든 상세 정보를 포함한 데이터 조회"""
    with conn.session as s:
        # 모든 컬럼 선택 (LEFT JOIN으로 변경하여 제품 정보 없어도 조회 가능하도록)
        base_sql = """
            SELECT s.*, p.product_name, p.product_code as db_prod_code, p.unit as p_unit
            FROM import_schedules s
            LEFT JOIN products p ON s.product_id = p.product_id
        """
        if status_filter != 'ALL':
            base_sql += f" WHERE s.status = '{status_filter}'"
        
        base_sql += " ORDER BY s.expected_date ASC, s.id DESC"
        
        df = pd.DataFrame(s.execute(text(base_sql)).fetchall())
        return df

def save_full_schedule(data, sid=None):
    """상세 정보 저장 (INSERT or UPDATE)"""
    try:
        with conn.session as s:
            cols = [
                'product_id', 'expected_date', 'quantity', 'note', 'status', 'size', 'supplier', 'unit_price', 'ck_code',
                'global_code', 'doojin_code', 'agency', 'agency_contract', 'origin', 'packing', 
                'open_qty', 'doc_qty', 'box_qty', 'unit2', 'open_amount', 'doc_amount',
                'tt_check', 'bank', 'usance', 'at_sight', 'open_date', 'lc_no', 'invoice_no', 'bl_no', 'lg_no', 'insurance',
                'customs_broker_date', 'etd', 'arrival_date', 'warehouse', 'actual_in_qty', 'destination',
                'doc_acceptance', 'acceptance_rate', 'maturity_date', 'ext_maturity_date', 'acceptance_fee', 'discount_fee',
                'payment_date', 'payment_amount', 'exchange_rate', 'balance', 'avg_exchange_rate',
                'clearance_info', 'declaration_info'
            ]
            
            # 숫자형 컬럼 리스트 (0으로 처리할 것들)
            numeric_cols = ['quantity', 'unit_price', 'open_qty', 'doc_qty', 'box_qty', 'open_amount', 'doc_amount', 
                            'actual_in_qty', 'acceptance_rate', 'acceptance_fee', 'discount_fee', 'payment_amount', 
                            'exchange_rate', 'balance', 'avg_exchange_rate']
            
            # JSON 컬럼 리스트
            json_cols = ['clearance_info', 'declaration_info']

            params = {}
            for k in cols:
                val = data.get(k)
                if k in numeric_cols:
                    if val is None or str(val).strip() == '':
                        params[k] = 0
                    else:
                        try: params[k] = float(str(val).replace(',', '').strip())
                        except: params[k] = 0
                elif k in json_cols:
                    # JSON 데이터 처리
                    if isinstance(val, (list, dict)):
                        params[k] = json.dumps(val, ensure_ascii=False)
                    elif isinstance(val, str) and (val.startswith('[') or val.startswith('{')):
                        params[k] = val # 이미 JSON 문자열인 경우
                    else:
                        params[k] = '[]' # 기본값
                else:
                    if val is None or str(val).strip() == '' or str(val).lower() == 'nan':
                        params[k] = None
                    else:
                        params[k] = val
            
            # status 값 강제 설정 (값이 없으면 PENDING)
            if not params.get('status'):
                params['status'] = 'PENDING'

            if sid:
                set_clause = ", ".join([f"{col} = :{col}::jsonb" if col in json_cols else f"{col} = :{col}" for col in cols])
                sql = f"UPDATE import_schedules SET {set_clause} WHERE id = :id"
                params['id'] = sid
                s.execute(text(sql), params)
                msg = "수정 완료"
            else:
                col_str = ", ".join(cols)
                val_str = ", ".join([f":{col}::jsonb" if col in json_cols else f":{col}" for col in cols])
                sql = f"INSERT INTO import_schedules ({col_str}) VALUES ({val_str})"
                # status 기본값 설정 로직은 위에서 처리됨
                s.execute(text(sql), params)
                msg = "등록 완료"
                
            s.commit()
        return True, msg
    except Exception as e: return False, str(e)

def delete_schedule(sid):
    try:
        with conn.session as s:
            s.execute(text("DELETE FROM import_schedules WHERE id = :sid"), {"sid": sid})
            s.commit()
        return True, "삭제 완료"
    except Exception as e: return False, str(e)

# --- 유틸리티 함수 ---
def safe_date_parse(val):
    if pd.isna(val) or str(val).strip() == '': return None
    try:
        if isinstance(val, datetime): return val.strftime('%Y-%m-%d')
        s_val = str(val).strip()
        if re.match(r'^\d{2}/\d{2}/\d{2}$', s_val): # 25/01/01
            dt = datetime.strptime(s_val, "%y/%m/%d")
            if dt.year < 2000: dt = dt.replace(year=dt.year+2000)
            return dt.strftime('%Y-%m-%d')
        if re.match(r'^\d{4}-\d{2}-\d{2}$', s_val): return datetime.strptime(s_val, "%Y-%m-%d").strftime('%Y-%m-%d')
        if re.match(r'^\d{4}\.\d{2}\.\d{2}$', s_val): return datetime.strptime(s_val, "%Y.%m.%d").strftime('%Y-%m-%d')
        if re.match(r'^\d{8}$', s_val): return datetime.strptime(s_val, "%Y%m%d").strftime('%Y-%m-%d')
        return pd.to_datetime(val).strftime('%Y-%m-%d')
    except: return None

def safe_float_parse(val):
    if pd.isna(val) or str(val).strip() == '': return 0.0
    try: return float(str(val).replace(',', '').replace(' ', '').strip())
    except: return 0.0

def parse_import_full_excel(df):
    """
    '수입' 탭(상세 장부) 구조의 엑셀/CSV 파일 파싱
    헤더를 찾아 컬럼 매핑 후 데이터 추출
    """
    valid_data = []
    errors = []
    
    p_df = get_products_df()
    if p_df.empty: return [], ["시스템에 등록된 품목이 없습니다."]
    product_map = {str(row['품목명']).replace(" ", "").lower(): row['ID'] for _, row in p_df.iterrows()}
    
    # 1. 헤더 행 찾기 (스코어링 방식)
    keywords = ['CK', '관리번호', '품명', '수량', '단가', '글로벌', '두진', '입고일', 'ETA']
    
    col_str = " ".join([str(c).strip() for c in df.columns])
    score_cols = 0
    for k in keywords:
        if k in col_str: score_cols += 1
    
    data_df = pd.DataFrame()
    header_row_idx = -1

    # 만약 현재 컬럼명이 헤더로 보인다면 (키워드 2개 이상 포함)
    if score_cols >= 2 and ('CK' in col_str or '관리번호' in col_str) and '품명' in col_str:
        data_df = df
    else:
        if df.empty: return [], ["파일 내용이 없습니다."]

        max_score = 0
        for i in range(min(20, len(df))):
            row_vals = [str(x).strip() for x in df.iloc[i].values if pd.notna(x)]
            row_str = " ".join(row_vals)
            score = 0
            for k in keywords:
                if k in row_str: score += 1
            
            if score > max_score and score >= 2:
                max_score = score
                header_row_idx = i
                
        if header_row_idx != -1:
            df.columns = df.iloc[header_row_idx]
            data_df = df.iloc[header_row_idx+1:].reset_index(drop=True)
        else:
            return [], ["헤더('CK', '품명' 등)를 찾을 수 없습니다."]

    # 컬럼 이름 정제 (줄바꿈, 공백 제거)
    data_df.columns = [str(c).replace('\n', '').replace('\r', '').replace(' ', '').strip() for c in data_df.columns]
    cols = list(data_df.columns)
    
    # 3. 컬럼 매핑
    def find_col(keywords):
        for c in cols:
            c_clean = str(c).upper().strip()
            for k in keywords:
                k_clean = k.upper().replace(" ", "").replace("\n", "")
                if k_clean in c_clean: return c
        return None

    col_map = {
        'ck': find_col(['CK', '관리번호']), 'global': find_col(['글로벌']), 'doojin': find_col(['두진']),
        'agency': find_col(['대행']), 'agency_contract': find_col(['대행계약서']),
        'supplier': find_col(['수출자', '수입자']), 'origin': find_col(['원산지']), 'name': find_col(['품명']),
        'size': find_col(['사이즈']), 'packing': find_col(['Packing']), 'open_qty': find_col(['오픈수량']),
        'unit': find_col(['단위']), 'doc_qty': find_col(['서류수량']), 'box_qty': find_col(['박스수량']),
        'price': find_col(['단가']), 'open_amt': find_col(['오픈금액']), 'doc_amt': find_col(['서류금액']),
        'tt': find_col(['T/T']), 'bank': find_col(['은행']), 'usance': find_col(['Usance']), 'at_sight': find_col(['AtSight']),
        'open_date': find_col(['개설일']), 'lc_no': find_col(['LCNo', 'L/C']), 'inv_no': find_col(['Invoice']),
        'bl_no': find_col(['BLNo', 'B/L']), 'lg_no': find_col(['LG', 'L/G']), 'insurance': find_col(['보험']),
        'broker_date': find_col(['관세사', '관세사발송일']), 'etd': find_col(['ETD']), 'eta': find_col(['ETA']),
        'arrival_date': find_col(['입고일']), 'wh': find_col(['창고']), 'real_in_qty': find_col(['실입고', '실입고수량']),
        'dest': find_col(['착지']), 'note': find_col(['비고']), 'doc_acc': find_col(['서류인수']),
        'acc_rate': find_col(['인수수수료율']), 'mat_date': find_col(['만기일']), 'ext_date': find_col(['연장만기일']),
        'acc_fee': find_col(['인수수수료']), 'dis_fee': find_col(['인수할인료']), 'pay_date': find_col(['결제일']),
        'pay_amt': find_col(['결제금액']), 'ex_rate': find_col(['환율']), 'balance': find_col(['잔액']), 'avg_ex': find_col(['평균환율'])
    }
    
    if col_map['agency'] and '계약서' in str(col_map['agency']):
        col_map['agency'] = None
        for c in cols:
            if '대행' in str(c) and '계약서' not in str(c): col_map['agency'] = c; break

    try:
        if col_map['price']:
            idx = cols.index(col_map['price'])
            col_map['unit2'] = cols[idx+1] if idx + 1 < len(cols) else None
        else: col_map['unit2'] = None
    except: col_map['unit2'] = None

    # 데이터 추출
    for idx, row in data_df.iterrows():
        if not col_map['name']: continue
        name_val = str(row.get(col_map['name'], '')).strip()
        if not name_val or name_val.lower() == 'nan': continue
        
        search_key = name_val.replace(" ", "").lower()
        pid = product_map.get(search_key)
        ck_val = str(row.get(col_map['ck'], '')).strip() if col_map['ck'] else ""
        if ck_val.lower() == 'nan': ck_val = ""
        
        if not pid:
            errors.append(f"[행 {idx+2}] 알 수 없는 품목: '{name_val}' (CK: {ck_val})")
            continue
            
        try:
            # 헬퍼 함수
            def get_val(key, parser=str):
                col = col_map.get(key)
                if col:
                    val = row.get(col)
                    return parser(val)
                return 0.0 if parser == safe_float_parse else (None if parser == safe_date_parse else '')

            # 통관 정보 추출
            clearance_list = []
            for i in range(1, 11):
                suffix = str(i) if i > 1 else ""
                c_date_col = find_col([f"통관일자{suffix}", f"통관일자 {suffix}"])
                c_qty_col = find_col([f"통관수량{suffix}", f"통관 수량{suffix}"])
                c_rate_col = find_col([f"통관환율{suffix}", f"통관 환율{suffix}"])
                
                if c_date_col or c_qty_col:
                    d_val = safe_date_parse(row.get(c_date_col)) if c_date_col else None
                    q_val = safe_float_parse(row.get(c_qty_col)) if c_qty_col else 0.0
                    r_val = safe_float_parse(row.get(c_rate_col)) if c_rate_col else 0.0
                    if d_val or q_val > 0:
                        clearance_list.append({"date": d_val, "qty": q_val, "rate": r_val})

            # 수입신고 정보 추출
            declaration_list = []
            for i in range(1, 11):
                suffix = str(i) if i > 1 else ""
                d_date_col = find_col([f"신고일{suffix}", f"신고일 {suffix}"])
                d_no_col = find_col([f"신고번호{suffix}", f"신고번호 {suffix}"])
                
                if d_date_col or d_no_col:
                    date_val = safe_date_parse(row.get(d_date_col)) if d_date_col else None
                    no_val = str(row.get(d_no_col, '')).strip() if d_no_col else ''
                    if date_val or no_val:
                        declaration_list.append({"date": date_val, "no": no_val})

            data = {
                'product_id': pid, 'ck_code': ck_val,
                'global_code': get_val('global'), 'doojin_code': get_val('doojin'),
                'agency': get_val('agency'), 'agency_contract': get_val('agency_contract'),
                'supplier': get_val('supplier'), 'origin': get_val('origin'),
                'size': get_val('size'), 'packing': get_val('packing'),
                'open_qty': get_val('open_qty', safe_float_parse),
                'quantity': get_val('open_qty', safe_float_parse),
                'doc_qty': get_val('doc_qty', safe_float_parse),
                'box_qty': get_val('box_qty', safe_float_parse),
                'unit2': get_val('unit2'),
                'unit_price': get_val('price', safe_float_parse),
                'open_amount': get_val('open_amt', safe_float_parse),
                'doc_amount': get_val('doc_amt', safe_float_parse),
                'tt_check': get_val('tt'), 'bank': get_val('bank'),
                'usance': get_val('usance'), 'at_sight': get_val('at_sight'),
                'open_date': get_val('open_date', safe_date_parse),
                'lc_no': get_val('lc_no'), 'invoice_no': get_val('inv_no'),
                'bl_no': get_val('bl_no'), 'lg_no': get_val('lg_no'), 'insurance': get_val('insurance'),
                'customs_broker_date': get_val('broker_date', safe_date_parse),
                'etd': get_val('etd', safe_date_parse),
                'expected_date': get_val('eta', safe_date_parse) or get_kst_today(),
                'arrival_date': get_val('arrival_date', safe_date_parse),
                'warehouse': get_val('wh'), 
                'actual_in_qty': get_val('real_in_qty', safe_float_parse),
                'destination': get_val('dest'), 'note': get_val('note'),
                'doc_acceptance': get_val('doc_acc', safe_date_parse),
                'acceptance_rate': get_val('acc_rate', safe_float_parse),
                'maturity_date': get_val('mat_date', safe_date_parse),
                'ext_maturity_date': get_val('ext_date', safe_date_parse),
                'acceptance_fee': get_val('acc_fee', safe_float_parse),
                'discount_fee': get_val('dis_fee', safe_float_parse),
                'payment_date': get_val('pay_date', safe_date_parse),
                'payment_amount': get_val('pay_amt', safe_float_parse),
                'exchange_rate': get_val('ex_rate', safe_float_parse),
                'balance': get_val('balance', safe_float_parse),
                'avg_exchange_rate': get_val('avg_ex', safe_float_parse),
                'clearance_info': clearance_list,
                'declaration_info': declaration_list,
                'status': 'PENDING'
            }
            data['unit'] = str(row.get(col_map.get('unit'), '')).strip() if col_map.get('unit') else ''

            for k, v in data.items():
                if isinstance(v, str) and (v.lower() == 'nan' or v.lower() == 'nat'): data[k] = ''
            valid_data.append(data)
        except Exception as e:
            errors.append(f"[행 {idx+2}] 데이터 파싱 오류: {str(e)}")
            
    return valid_data, errors

# ==========================================
# 2. 메인 UI 구성
# ==========================================

st.title("🚢 수입 관리 시스템")

tab_status, tab_ledger, tab_manage, tab_product = st.tabs(["📊 수입진행상황", "📒 수입장부 (상세)", "📝 등록 및 관리", "📦 품목 관리"])

# --- TAB 1: 수입진행상황 (HTML 뷰) ---
with tab_status:
    st.markdown("### 📅 수입 진행 현황판")
    
    df = get_full_schedule_data('ALL')
    
    if df.empty:
        st.info("등록된 수입 일정이 없습니다.")
    else:
        df['eta_str'] = pd.to_datetime(df['expected_date']).dt.strftime('%y/%m/%d')
        grouped = df.groupby('eta_str', sort=False)
        
        # HTML 렌더링 (들여쓰기 제거하여 코드 블록 인식 방지)
        html_content = """<table style="width:100%; border-collapse: collapse; font-size:13px; text-align:center;"><thead><tr style="background-color:#f1f3f5; border-bottom:2px solid #dee2e6;"><th style="padding:8px;">입항일</th><th style="padding:8px;">공급사</th><th style="padding:8px;">품명</th><th style="padding:8px;">CK</th><th style="padding:8px;">사이즈</th><th style="padding:8px;">단가</th><th style="padding:8px;">수량</th><th style="padding:8px;">상태</th></tr></thead><tbody>"""
        
        for date_str, group in grouped:
            html_content += f"""<tr style="background-color:#e7f5ff; border-top:1px solid #dee2e6; border-bottom:1px solid #dee2e6;"><td colspan="8" style="padding:6px; font-weight:bold; text-align:left; padding-left:15px;">📅 {date_str} (총 {len(group)}건)</td></tr>"""
            
            for _, row in group.iterrows():
                status_cls = "status-pending" if row['status'] == 'PENDING' else ("status-arrived" if row['status'] == 'ARRIVED' else "status-canceled")
                status_txt = "진행중" if row['status'] == 'PENDING' else ("입고완료" if row['status'] == 'ARRIVED' else "취소")
                
                ck_val = row['ck_code'] if row['ck_code'] else "-"
                supp_val = row['supplier'] if row['supplier'] else "-"
                size_val = row['size'] if row['size'] else "-"
                qty_val = f"{int(row['quantity']):,}" if row['quantity'] else "0"
                price_val = f"${float(row['unit_price']):.2f}" if row['unit_price'] else "-"
                
                html_content += f"""<tr style="border-bottom:1px solid #f1f3f5;"><td style="padding:6px; color:#868e96;">{date_str}</td><td style="padding:6px;">{supp_val}</td><td style="padding:6px; font-weight:bold;">{row['product_name']}</td><td style="padding:6px; font-family:monospace; color:#495057;">{ck_val}</td><td style="padding:6px;">{size_val}</td><td style="padding:6px;">{price_val}</td><td style="padding:6px; font-weight:bold; color:#1c7ed6;">{qty_val}</td><td style="padding:6px;"><span class="status-badge {status_cls}">{status_txt}</span></td></tr>"""
        
        html_content += "</tbody></table>"
        st.markdown(html_content, unsafe_allow_html=True)

# --- TAB 2: 수입장부 (상세 표) ---
with tab_ledger:
    st.markdown("### 📒 수입장부 상세 내역")
    
    col_l1, col_l2 = st.columns([1, 5])
    with col_l1:
        view_filter = st.selectbox("상태 필터", ["전체", "진행중", "완료/취소"])
    
    # 검색 기능 추가
    search_query = st.text_input("🔍 검색 (관리번호, 품명, 수출자)", placeholder="검색어 입력...")

    db_filter = 'ALL'
    if view_filter == "진행중": db_filter = 'PENDING'
    elif view_filter == "완료/취소": db_filter = 'ARRIVED'
    
    df_ledger = get_full_schedule_data(db_filter)
    
    if df_ledger.empty:
        st.info("데이터가 없습니다.")
    else:
        # 검색 필터링 적용
        if search_query:
            query = search_query.lower()
            df_ledger = df_ledger[
                df_ledger['ck_code'].astype(str).str.lower().str.contains(query) |
                df_ledger['product_name'].astype(str).str.lower().str.contains(query) |
                df_ledger['supplier'].astype(str).str.lower().str.contains(query)
            ]

        cols_map = {
            'ck_code': 'CK관리번호', 'global_code': '글로벌', 'doojin_code': '두진',
            'supplier': '수출자', 'origin': '원산지', 'product_name': '품명', 'size': '사이즈',
            'packing': 'Packing', 'quantity': '오픈수량', 'unit_price': '단가', 'unit2': '단위2',
            'open_amount': '오픈금액', 'lc_no': 'L/C No', 'bl_no': 'B/L No',
            'etd': 'ETD', 'expected_date': 'ETA', 'arrival_date': '실입고일', 'warehouse': '창고',
            'doc_acceptance': '서류인수일', 'maturity_date': '만기일', 'payment_date': '결제일',
            'status': '상태', 'note': '비고'
        }
        
        avail_cols = [c for c in cols_map.keys() if c in df_ledger.columns]
        display_df = df_ledger[avail_cols].rename(columns=cols_map)
        
        if not display_df.empty:
            display_df = display_df.sort_values(by='CK관리번호', ascending=False)
            st.dataframe(
                display_df, 
                use_container_width=True, 
                height=700,
                hide_index=True
            )
        else:
            st.info("검색 결과가 없습니다.")

# --- TAB 3: 등록 및 관리 ---
with tab_manage:
    col_list, col_form = st.columns([1, 2])
    
    with col_list:
        sub_t1, sub_t2 = st.tabs(["목록 선택", "엑셀 일괄 등록"])
        
        with sub_t1:
            st.subheader("등록 건 목록")
            df_list = get_full_schedule_data('ALL')
            
            search_txt = st.text_input("🔍 검색 (CK, 품명 등)", key="list_search")
            if not df_list.empty and search_txt:
                mask = df_list.apply(lambda x: x.astype(str).str.contains(search_txt, case=False).any(), axis=1)
                df_list = df_list[mask]
            
            if st.button("➕ 신규 등록 (빈 양식)", type="primary", use_container_width=True):
                st.session_state['edit_mode'] = 'new'
                st.session_state['selected_data'] = None
                # 동적 필드 초기화
                st.session_state['clearance_list'] = []
                st.session_state['declaration_list'] = []
                st.rerun()
                
            st.markdown("---")
            if not df_list.empty:
                for idx, row in df_list.iterrows():
                    label = f"**[{row['ck_code'] or 'NO-CK'}]** {row['product_name']}"
                    sub = f"{row['supplier'] or '-'} | ETA: {row['expected_date']} | {row['status']}"
                    with st.container(border=True):
                        st.markdown(label)
                        st.caption(sub)
                        if st.button("상세/수정", key=f"sel_{row['id']}", use_container_width=True):
                            st.session_state['edit_mode'] = 'edit'
                            st.session_state['selected_data'] = row.to_dict()
                            
                            # JSON 파싱하여 세션에 로드
                            try:
                                clr_info = row['clearance_info']
                                st.session_state['clearance_list'] = json.loads(clr_info) if clr_info else []
                            except: st.session_state['clearance_list'] = []
                            
                            try:
                                decl_info = row['declaration_info']
                                st.session_state['declaration_list'] = json.loads(decl_info) if decl_info else []
                            except: st.session_state['declaration_list'] = []
                            
                            st.rerun()
            else: st.info("데이터가 없습니다.")
        
        with sub_t2:
            st.subheader("엑셀 파일 업로드")
            st.markdown("""
            **💡 업로드 가이드**
            1. 아래 양식 다운로드 버튼을 눌러 템플릿을 받으세요.
            2. 템플릿의 **헤더(첫 줄)를 유지**한 채 데이터를 입력하세요.
            3. **품명**은 시스템에 등록된 것과 정확히 일치해야 합니다.
            """)
            
            # 양식 다운로드 (간단한 CSV 생성)
            sample_data = {
                "CK": ["CK-SAMPLE"], "글로벌": [""], "두진": [""], "대행": [""], "대행계약서": [""], "수출자": ["Supplier A"],
                "원산지": ["Country A"], "품명": ["Sample Product"], "사이즈": ["Size A"], "Packing": [""], "오픈수량": [100],
                "단위": ["BOX"], "서류수량": [""], "박스수량": [""], "단가": [10.5], "단위2": ["KG"], "오픈금액": [""], "서류금액": [""],
                "T/T": [""], "은행": [""], "Usance": [""], "At Sight": [""], "개설일": [""], "L/C No": [""], "Invoice": [""], "B/L": [""],
                "L/G": [""], "보험": [""], "관세사": [""], "ETD": [""], "ETA": ["2025-01-01"], "입고일": [""], "창고": [""], "실입고": [""],
                "착지": [""], "비고": [""], "서류인수": [""], "인수수수료율": [""], "만기일": [""], "연장만기일": [""], "인수수수료": [""],
                "인수할인료": [""], "결제일": [""], "결제금액": [""], "환율": [""], "잔액": [""], "평균환율": [""]
            }
            sample_df = pd.DataFrame(sample_data)
            csv_buffer = io.BytesIO()
            sample_df.to_csv(csv_buffer, index=False, encoding='cp949')
            st.download_button("📥 등록 양식 다운로드 (CSV)", csv_buffer.getvalue(), "import_template.csv", "text/csv")

            up_file = st.file_uploader("파일 선택", type=['csv', 'xlsx'])
            if up_file:
                if st.button("분석 및 등록 시작", use_container_width=True):
                    try:
                        # 파일 포맷 및 인코딩 처리
                        if up_file.name.endswith('.csv'):
                            try:
                                # utf-8 시도
                                df_up = pd.read_csv(up_file)
                            except:
                                # 실패 시 cp949 시도 (한글 윈도우)
                                up_file.seek(0)
                                df_up = pd.read_csv(up_file, encoding='cp949')
                        else:
                            df_up = pd.read_excel(up_file)
                            
                        valid_rows, err_list = parse_import_full_excel(df_up)
                        
                        if err_list:
                            st.error(f"{len(err_list)}건의 에러가 있습니다.")
                            with st.expander("에러 상세 보기"):
                                for e in err_list: st.write(f"- {e}")
                        
                        if valid_rows:
                            st.success(f"{len(valid_rows)}건의 유효 데이터를 찾았습니다.")
                            prog = st.progress(0)
                            cnt = 0
                            for i, d in enumerate(valid_rows):
                                ok, _ = save_full_schedule(d)
                                if ok: cnt += 1
                                prog.progress((i+1)/len(valid_rows))
                            st.toast(f"{cnt}건 일괄 등록 완료!")
                            time.sleep(1)
                            st.rerun()
                    except Exception as e:
                        st.error(f"오류 발생: {e}")

    # [우측] 상세 입력 폼
    with col_form:
        edit_mode = st.session_state.get('edit_mode', 'new')
        data = st.session_state.get('selected_data', {})
        
        # 동적 리스트 초기화
        if 'clearance_list' not in st.session_state: st.session_state['clearance_list'] = []
        if 'declaration_list' not in st.session_state: st.session_state['declaration_list'] = []
        
        title_prefix = "수정" if edit_mode == 'edit' else "신규 등록"
        st.subheader(f"📝 상세 정보 {title_prefix}")
        
        if edit_mode == 'edit' and not data:
            st.info("좌측 목록에서 항목을 선택해주세요.")
        else:
            with st.form("detail_form"):
                st.markdown("<div class='form-header'>기본 식별 정보</div>", unsafe_allow_html=True)
                c1, c2, c3, c4 = st.columns(4)
                ck_code = c1.text_input("CK 관리번호", value=data.get('ck_code', ''))
                global_code = c2.text_input("글로벌 번호", value=data.get('global_code', ''))
                doojin_code = c3.text_input("두진 번호", value=data.get('doojin_code', ''))
                
                p_df = get_products_df()
                p_opts = {row['ID']: f"[{row['카테고리']}] {row['품목명']} ({row['품목코드']})" for _, row in p_df.iterrows()}
                def_pid = data.get('product_id')
                if def_pid not in p_opts: def_pid = None
                opt_keys = list(p_opts.keys())
                sel_idx = opt_keys.index(def_pid) if def_pid in opt_keys else 0
                sel_pid = c4.selectbox("품목 (필수)", options=opt_keys, format_func=lambda x: p_opts[x], index=sel_idx)

                st.markdown("<div class='form-header'>계약 및 물품 정보</div>", unsafe_allow_html=True)
                c1, c2, c3, c4 = st.columns(4)
                agency = c1.text_input("대행사", value=data.get('agency', ''))
                agency_contract = c2.text_input("대행 계약서", value=data.get('agency_contract', ''))
                supplier = c3.text_input("수출자(수입자)", value=data.get('supplier', ''))
                origin = c4.text_input("원산지", value=data.get('origin', ''))
                
                c1, c2, c3, c4 = st.columns(4)
                size = c1.text_input("사이즈", value=data.get('size', ''))
                packing = c2.text_input("Packing", value=data.get('packing', ''))
                unit_price = c3.number_input("단가 (USD)", value=float(data.get('unit_price') or 0.0), step=0.01, format="%.2f")
                unit2 = c4.text_input("단가 단위", value=data.get('unit2', 'kg'))

                c1, c2, c3, c4 = st.columns(4)
                quantity = c1.number_input("오픈 수량", value=float(data.get('quantity') or 0.0))
                doc_qty = c2.number_input("서류 수량", value=float(data.get('doc_qty') or 0.0))
                box_qty = c3.number_input("박스 수량", value=float(data.get('box_qty') or 0.0))
                open_amount = c4.number_input("오픈 금액", value=float(data.get('open_amount') or 0.0))

                st.markdown("<div class='form-header'>L/C 및 서류 정보</div>", unsafe_allow_html=True)
                c1, c2, c3, c4 = st.columns(4)
                tt_check = c1.text_input("T/T 여부", value=data.get('tt_check', ''))
                bank = c2.text_input("개설 은행", value=data.get('bank', ''))
                lc_no = c3.text_input("L/C No.", value=data.get('lc_no', ''))
                open_date = c4.date_input("개설일", value=data.get('open_date'))

                c1, c2, c3, c4 = st.columns(4)
                invoice_no = c1.text_input("Invoice No.", value=data.get('invoice_no', ''))
                bl_no = c2.text_input("B/L No.", value=data.get('bl_no', ''))
                lg_no = c3.text_input("L/G", value=data.get('lg_no', ''))
                insurance = c4.text_input("보험", value=data.get('insurance', ''))

                st.markdown("<div class='form-header'>일정 및 물류 정보</div>", unsafe_allow_html=True)
                c1, c2, c3, c4 = st.columns(4)
                etd = c1.date_input("ETD (출항)", value=data.get('etd'))
                eta = c2.date_input("ETA (입항/예정일)", value=data.get('expected_date') or get_kst_today())
                arrival_date = c3.date_input("실 입고일", value=data.get('arrival_date'))
                customs_broker_date = c4.date_input("관세사 전달일", value=data.get('customs_broker_date'))

                c1, c2, c3 = st.columns(3)
                warehouse = c1.text_input("창고", value=data.get('warehouse', ''))
                destination = c2.text_input("착지", value=data.get('destination', ''))
                actual_in_qty = c3.number_input("실 입고 수량", value=float(data.get('actual_in_qty') or 0.0))

                st.markdown("<div class='form-header'>결제 정보</div>", unsafe_allow_html=True)
                c1, c2, c3, c4 = st.columns(4)
                doc_acceptance = c1.date_input("서류 인수일", value=data.get('doc_acceptance'))
                maturity_date = c2.date_input("만기일", value=data.get('maturity_date'))
                payment_date = c3.date_input("결제일", value=data.get('payment_date'))
                payment_amount = c4.number_input("결제 금액", value=float(data.get('payment_amount') or 0.0))

                # --- 통관 정보 (동적 추가) ---
                st.markdown("<div class='form-header'>통관 정보 (N차 가능)</div>", unsafe_allow_html=True)
                # 폼 내부에서는 state 조작이 제한적이므로, 간단히 N개 슬롯을 보여주거나 
                # form 밖에서 관리해야 하지만, 여기선 JSON 데이터를 텍스트로 보여주고 수정하는 방식이 아닌
                # 폼 제출 시 기존 리스트 + 추가된 입력을 합치는 방식으로 구현 
                # (Streamlit 폼 한계상 5개 고정 슬롯 제공 방식 사용)
                
                clr_data = st.session_state['clearance_list']
                new_clr_list = []
                
                for i in range(5):
                    # 기존 데이터 있으면 채우기
                    def_date = None
                    def_qty = 0.0
                    def_rate = 0.0
                    if i < len(clr_data):
                        try:
                            if clr_data[i].get('date'): def_date = datetime.strptime(clr_data[i]['date'], '%Y-%m-%d').date()
                            def_qty = float(clr_data[i].get('qty', 0))
                            def_rate = float(clr_data[i].get('rate', 0))
                        except: pass
                    
                    cc1, cc2, cc3 = st.columns(3)
                    cd = cc1.date_input(f"[{i+1}] 통관일자", value=def_date, key=f"clr_d_{i}")
                    cq = cc2.number_input(f"[{i+1}] 통관수량", value=def_qty, key=f"clr_q_{i}")
                    cr = cc3.number_input(f"[{i+1}] 통관환율", value=def_rate, key=f"clr_r_{i}")
                    
                    if cd or cq > 0:
                        new_clr_list.append({"date": str(cd) if cd else None, "qty": cq, "rate": cr})

                # --- 수입신고 정보 (동적 추가) ---
                st.markdown("<div class='form-header'>수입신고 정보 (N차 가능)</div>", unsafe_allow_html=True)
                decl_data = st.session_state['declaration_list']
                new_decl_list = []
                
                for i in range(5):
                    d_def_date = None
                    d_def_no = ""
                    if i < len(decl_data):
                        try:
                            if decl_data[i].get('date'): d_def_date = datetime.strptime(decl_data[i]['date'], '%Y-%m-%d').date()
                            d_def_no = decl_data[i].get('no', "")
                        except: pass
                        
                    dc1, dc2 = st.columns(2)
                    dd = dc1.date_input(f"[{i+1}] 신고일", value=d_def_date, key=f"decl_d_{i}")
                    dn = dc2.text_input(f"[{i+1}] 신고번호", value=d_def_no, key=f"decl_n_{i}")
                    
                    if dd or dn:
                        new_decl_list.append({"date": str(dd) if dd else None, "no": dn})

                st.markdown("<div class='form-header'>기타</div>", unsafe_allow_html=True)
                note = st.text_area("비고", value=data.get('note', ''))
                status = st.selectbox("진행 상태", ["PENDING", "ARRIVED", "CANCELED"], index=["PENDING", "ARRIVED", "CANCELED"].index(data.get('status', 'PENDING')))

                c_submit, c_del = st.columns([4, 1])
                with c_submit:
                    if st.form_submit_button("💾 정보 저장", type="primary", use_container_width=True):
                        save_data = {
                            'ck_code': ck_code, 'global_code': global_code, 'doojin_code': doojin_code,
                            'product_id': sel_pid, 'agency': agency, 'agency_contract': agency_contract,
                            'supplier': supplier, 'origin': origin, 'size': size, 'packing': packing,
                            'unit_price': unit_price, 'unit2': unit2, 
                            'quantity': quantity, 'doc_qty': doc_qty, 'box_qty': box_qty,
                            'open_amount': open_amount, 
                            'tt_check': tt_check, 'bank': bank, 'lc_no': lc_no, 'open_date': open_date,
                            'invoice_no': invoice_no, 'bl_no': bl_no, 'lg_no': lg_no, 'insurance': insurance,
                            'etd': etd, 'expected_date': eta, 'arrival_date': arrival_date, 'customs_broker_date': customs_broker_date,
                            'warehouse': warehouse, 'destination': destination, 'actual_in_qty': actual_in_qty,
                            'doc_acceptance': doc_acceptance, 'maturity_date': maturity_date, 'payment_date': payment_date,
                            'payment_amount': payment_amount, 'note': note, 'status': status,
                            'clearance_info': new_clr_list, 'declaration_info': new_decl_list
                        }
                        sid = data.get('id') if edit_mode == 'edit' else None
                        succ, msg = save_full_schedule(save_data, sid)
                        if succ:
                            st.success(msg)
                            time.sleep(1)
                            st.rerun()
                        else: st.error(f"저장 실패: {msg}")
                
                with c_del:
                    if edit_mode == 'edit':
                        if st.form_submit_button("🗑️ 삭제"):
                            delete_schedule(data['id'])
                            st.session_state['edit_mode'] = 'new'
                            st.session_state['selected_data'] = None
                            st.rerun()

# --- TAB 4: 품목 관리 (DB) ---
with tab_product:
    st.markdown("### 📦 시스템 품목 관리")
    col_p1, col_p2 = st.columns([1, 2])
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
                else: st.warning("코드와 품목명은 필수입니다.")
    
    with col_p2:
        st.markdown("#### 등록된 품목 리스트")
        curr_prods = get_products_df()
        if not curr_prods.empty:
            st.dataframe(curr_prods, use_container_width=True, hide_index=True)
        else: st.info("등록된 품목이 없습니다.")