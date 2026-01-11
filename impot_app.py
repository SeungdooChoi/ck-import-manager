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
    .block-container { max-width: 95% !important; padding-top: 1rem; }
    .status-pending { color: #f59f00; font-weight: bold; }
    .status-arrived { color: #0ca678; font-weight: bold; }
    .status-canceled { color: #fa5252; font-weight: bold; }
    div[data-testid="stExpander"] { border: 1px solid #dee2e6; border-radius: 4px; }
    
    /* 테이블 스타일 */
    .dataframe { font-size: 11px !important; }
    
    .metric-box {
        background-color: #f1f3f5;
        border: 1px solid #dee2e6;
        padding: 10px;
        border-radius: 8px;
        text-align: center;
        font-size: 14px;
    }
    
    .form-header {
        font-weight: bold;
        font-size: 1.1em;
        margin-top: 20px;
        margin-bottom: 10px;
        border-bottom: 2px solid #eee;
        padding-bottom: 5px;
        color: #333;
    }
</style>
""", unsafe_allow_html=True)

# DB 연결 및 스키마 업데이트 (수입 탭의 모든 컬럼 반영)
try:
    conn = st.connection("supabase", type="sql")
    with conn.session as s:
        # 기존 기본 컬럼 외 추가 컬럼
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS ck_code TEXT;"))
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS size TEXT;"))
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS unit_price NUMERIC;"))
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS supplier TEXT;"))
        
        # '수입' 탭 상세 컬럼 추가
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS global_code TEXT;")) # 글로벌
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS doojin_code TEXT;")) # 두진
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS agency TEXT;")) # 대행
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS agency_contract TEXT;")) # 대행계약서
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS origin TEXT;")) # 원산지
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS packing TEXT;")) # Packing
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS open_qty NUMERIC;")) # 오픈 수량
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS doc_qty NUMERIC;")) # 서류 수량
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS box_qty NUMERIC;")) # 박스 수량
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS unit2 TEXT;")) # 단위2 (KG 등)
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS open_amount NUMERIC;")) # 오픈 금액
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS doc_amount NUMERIC;")) # 서류 금액
        
        # L/C 및 금융 정보
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS tt_check TEXT;")) # T/T
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS bank TEXT;")) # 은행
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS usance TEXT;")) # Usance
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS at_sight TEXT;")) # At Sight
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS open_date DATE;")) # 개설일
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS lc_no TEXT;")) # L/C No.
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS invoice_no TEXT;")) # Invoice No.
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS bl_no TEXT;")) # B/L No.
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS lg_no TEXT;")) # L/G
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS insurance TEXT;")) # 보험
        
        # 물류 및 일정 정보
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS customs_broker_date DATE;")) # 관세사 발송일
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS etd DATE;")) # ETD
        # expected_date는 ETA/입항일로 사용
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS arrival_date DATE;")) # 입고일 (실제)
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS warehouse TEXT;")) # 창고
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS actual_in_qty NUMERIC;")) # 실입고 수량
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS destination TEXT;")) # 착지
        
        # 결제 정보
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS doc_acceptance DATE;")) # 서류인수
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS acceptance_rate NUMERIC;")) # 인수 수수료율
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS maturity_date DATE;")) # 만기일
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS ext_maturity_date DATE;")) # 연장 만기일
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS acceptance_fee NUMERIC;")) # 인수 수수료
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS discount_fee NUMERIC;")) # 인수 할인료
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS payment_date DATE;")) # 결제일
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS payment_amount NUMERIC;")) # 결제금액
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS exchange_rate NUMERIC;")) # 환율
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS balance NUMERIC;")) # 잔액
        s.execute(text("ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS avg_exchange_rate NUMERIC;")) # 평균환율
        
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
        # 모든 컬럼 선택
        base_sql = """
            SELECT s.*, p.product_name, p.product_code as db_prod_code, p.unit as p_unit
            FROM import_schedules s
            JOIN products p ON s.product_id = p.product_id
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
                'payment_date', 'payment_amount', 'exchange_rate', 'balance', 'avg_exchange_rate'
            ]
            
            # 파라미터 딕셔너리 구성 (None 값 처리 및 기본값 설정)
            params = {}
            for k in cols:
                val = data.get(k)
                # 숫자형 컬럼 리스트
                numeric_cols = ['quantity', 'unit_price', 'open_qty', 'doc_qty', 'box_qty', 'open_amount', 'doc_amount', 
                                'actual_in_qty', 'acceptance_rate', 'acceptance_fee', 'discount_fee', 'payment_amount', 
                                'exchange_rate', 'balance', 'avg_exchange_rate']
                
                if k in numeric_cols:
                    if val is None or val == '':
                        params[k] = 0  # 숫자형 컬럼에 값이 없으면 0으로 저장
                    else:
                        params[k] = val
                else:
                    # 문자열/날짜 컬럼
                    if val is None or val == '':
                        params[k] = None # DB NULL 처리를 위해 None
                    else:
                        params[k] = val

            if sid:
                # UPDATE
                set_clause = ", ".join([f"{col} = :{col}" for col in cols])
                sql = f"UPDATE import_schedules SET {set_clause} WHERE id = :id"
                params['id'] = sid
                s.execute(text(sql), params)
                msg = "수정 완료"
            else:
                # INSERT
                col_str = ", ".join(cols)
                val_str = ", ".join([f":{col}" for col in cols])
                sql = f"INSERT INTO import_schedules ({col_str}) VALUES ({val_str})"
                # status 기본값 설정 (신규 등록시)
                if 'status' not in params or not params['status']:
                    params['status'] = 'PENDING'
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

def update_schedule_status(sid, new_status):
    try:
        with conn.session as s:
            s.execute(text("UPDATE import_schedules SET status = :st WHERE id = :sid"), {"st": new_status, "sid": sid})
            s.commit()
        return True, "상태 변경 완료"
    except Exception as e: return False, str(e)

def safe_date_parse(val):
    """다양한 날짜 형식을 date 객체로 변환"""
    if pd.isna(val) or str(val).strip() == '': return None
    try:
        if isinstance(val, datetime): return val.date()
        s_val = str(val).strip()
        
        # 엑셀 날짜 형식 다양하게 처리
        # 25/01/01
        if re.match(r'^\d{2}/\d{2}/\d{2}$', s_val): 
            dt = datetime.strptime(s_val, "%y/%m/%d")
            if dt.year < 2000: dt = dt.replace(year=dt.year+2000)
            return dt.date()
        # 2025-01-01
        if re.match(r'^\d{4}-\d{2}-\d{2}$', s_val):
            return datetime.strptime(s_val, "%Y-%m-%d").date()
        # 2025.01.01
        if re.match(r'^\d{4}\.\d{2}\.\d{2}$', s_val): 
            return datetime.strptime(s_val, "%Y.%m.%d").date()
        # 20250101
        if re.match(r'^\d{8}$', s_val): 
            return datetime.strptime(s_val, "%Y%m%d").date()
            
        return pd.to_datetime(val).date()
    except:
        return None

def safe_float_parse(val):
    """문자열/숫자를 float으로 변환 (쉼표, 공백 제거)"""
    if pd.isna(val) or str(val).strip() == '': return 0.0
    try:
        s_val = str(val).replace(',', '').replace(' ', '').strip()
        return float(s_val)
    except: return 0.0

def parse_import_full_excel(df):
    """
    '수입' 탭(상세 장부) 구조의 엑셀/CSV 파일 파싱
    헤더를 찾아 컬럼 매핑 후 데이터 추출
    """
    valid_data = []
    errors = []
    
    # DB 품목 맵핑 (품명 -> ID)
    p_df = get_products_df()
    if p_df.empty:
        return [], ["시스템에 등록된 품목이 없습니다. 품목 관리 탭에서 먼저 품목을 등록하세요."]
    
    # 공백 제거/소문자 변환 매핑
    product_map = {str(row['품목명']).replace(" ", "").lower(): row['ID'] for _, row in p_df.iterrows()}
    
    # 1. 헤더 행 찾기 로직 개선
    # 데이터프레임 전체를 순회하며 헤더 후보 찾기
    header_row_idx = -1
    for i, row in df.iterrows():
        # 행의 모든 값을 하나의 문자열로 합쳐서 키워드 검색
        # NaN 값은 빈 문자열로 처리
        row_str = " ".join([str(x).strip() for x in row.values if pd.notna(x)])
        
        # 'CK' 와 '품명'이 포함된 행을 찾음 (조건 완화)
        if 'CK' in row_str and '품명' in row_str:
            header_row_idx = i
            break
            
    if header_row_idx == -1:
        # 두 번째 시도: '관리번호'가 있는 행 찾기 (제공된 CSV 예시 기준)
        for i, row in df.iterrows():
            row_str = " ".join([str(x).strip() for x in row.values if pd.notna(x)])
            if '관리번호' in row_str and '품명' in row_str:
                header_row_idx = i
                break
                
    if header_row_idx == -1:
        return [], ["헤더('CK' 또는 '관리번호', '품명')를 찾을 수 없습니다. '수입' 탭 양식인지 확인하세요."]

    # 2. 헤더 설정 및 데이터 슬라이싱
    # 헤더 행을 컬럼명으로 설정
    df.columns = df.iloc[header_row_idx]
    
    # 컬럼 이름의 공백/줄바꿈 정리 (매우 중요!)
    # \n, \r 등을 공백으로 치환하고 앞뒤 공백 제거
    df.columns = [str(c).replace('\n', ' ').replace('\r', '').strip() for c in df.columns]
    
    # 헤더 다음 행부터 데이터로 사용
    data_df = df.iloc[header_row_idx+1:].reset_index(drop=True)
    cols = list(data_df.columns)
    
    # 3. 컬럼 인덱스 매핑 (유사어 처리)
    def find_col(keywords):
        for c in cols:
            # 대소문자 무시, 공백 무시 비교
            c_clean = str(c).upper().replace(" ", "")
            for k in keywords:
                k_clean = k.upper().replace(" ", "")
                if k_clean in c_clean: return c
        return None

    # 제공된 CSV의 실제 컬럼명 기반 매핑 (줄바꿈/공백 대응 키워드)
    col_map = {
        'ck': find_col(['CK', '관리번호']),
        'global': find_col(['글로벌']),
        'doojin': find_col(['두진']),
        'agency': find_col(['대행']), # 대행계약서와 구분 주의 (순서상 먼저 매칭될 수 있음)
        # '대행'만 찾으면 '대행 계약서'도 매칭될 수 있으므로, 정확히 매칭하거나 제외 로직 필요
        # 여기서는 find_col이 부분 일치이므로 '대행'이 '대행계약서'보다 먼저 나오면 문제 없음.
        # 하지만 더 정확하게 하기 위해 리스트 순회하며 '대행'만 있는것 찾기 시도
        'agency_contract': find_col(['대행계약서', '대행 계약서']),
        'supplier': find_col(['수출자', '수입자']),
        'origin': find_col(['원산지']),
        'name': find_col(['품명']),
        'size': find_col(['사이즈']),
        'packing': find_col(['Packing']),
        'open_qty': find_col(['오픈수량', '오픈 수량']),
        'unit': find_col(['단위']), 
        'doc_qty': find_col(['서류수량', '서류 수량']),
        'box_qty': find_col(['박스수량', '박스 수량']),
        'price': find_col(['단가']), # '단가(USD)'
        # 단위2는 단가 바로 뒤 컬럼이라고 가정
        'open_amt': find_col(['오픈금액', '오픈 금액']),
        'doc_amt': find_col(['서류금액', '서류 금액']),
        'tt': find_col(['T/T']),
        'bank': find_col(['은행']),
        'usance': find_col(['Usance']),
        'at_sight': find_col(['AtSight', 'At Sight']),
        'open_date': find_col(['개설일']),
        'lc_no': find_col(['LCNo', 'L/C']),
        'inv_no': find_col(['Invoice']),
        'bl_no': find_col(['BLNo', 'B/L']),
        'lg_no': find_col(['LG', 'L/G']),
        'insurance': find_col(['보험']),
        'broker_date': find_col(['관세사', '관세사발송일']), 
        'etd': find_col(['ETD']),
        'eta': find_col(['ETA']),
        'arrival_date': find_col(['입고일']),
        'wh': find_col(['창고']),
        'real_in_qty': find_col(['실입고', '실입고수량']),
        'dest': find_col(['착지']),
        'note': find_col(['비고']),
        'doc_acc': find_col(['서류인수']),
        'acc_rate': find_col(['인수수수료율', '인수 수수료율']),
        'mat_date': find_col(['만기일']),
        'ext_date': find_col(['연장만기일', '연장 만기일']),
        'acc_fee': find_col(['인수수수료', '인수 수수료']), # 율 제외
        'dis_fee': find_col(['인수할인료', '인수 할인료']),
        'pay_date': find_col(['결제일']),
        'pay_amt': find_col(['결제금액', '결제 금액']),
        'ex_rate': find_col(['환율']),
        'balance': find_col(['잔액']),
        'avg_ex': find_col(['평균환율'])
    }
    
    # 'agency' 재확인: '대행계약서'가 '대행'으로 잘못 잡히는 경우 방지
    if col_map['agency'] and '계약서' in str(col_map['agency']):
        # '대행'이라는 글자만 있고 '계약서'는 없는 컬럼을 다시 찾음
        for c in cols:
            if '대행' in str(c) and '계약서' not in str(c):
                col_map['agency'] = c
                break

    # unit2 (단위2) 컬럼 위치 찾기 (단가 컬럼 오른쪽)
    try:
        price_col_name = col_map['price']
        if price_col_name:
            idx = cols.index(price_col_name)
            if idx + 1 < len(cols):
                col_map['unit2'] = cols[idx+1]
            else:
                col_map['unit2'] = None
        else:
            col_map['unit2'] = None
    except:
        col_map['unit2'] = None

    # 4. 데이터 파싱 루프
    for idx, row in data_df.iterrows():
        # 품명 없으면 스킵
        if not col_map['name']: continue # 품명 컬럼 자체를 못 찾음
        
        name_val = str(row.get(col_map['name'], '')).strip()
        if not name_val or name_val.lower() == 'nan': continue
        
        # 품목 매칭
        search_key = name_val.replace(" ", "").lower()
        pid = product_map.get(search_key)
        
        ck_val = str(row.get(col_map['ck'], '')).strip() if col_map['ck'] else ""
        if ck_val.lower() == 'nan': ck_val = ""
        
        if not pid:
            # 품목이 없으면 에러로 처리 (자동 등록 안 함)
            errors.append(f"[행 {idx+header_row_idx+2}] 알 수 없는 품목: '{name_val}' (CK: {ck_val})")
            continue
            
        try:
            data = {
                'product_id': pid,
                'ck_code': ck_val,
                'global_code': str(row.get(col_map['global'], '')).strip() if col_map['global'] else '',
                'doojin_code': str(row.get(col_map['doojin'], '')).strip() if col_map['doojin'] else '',
                'agency': str(row.get(col_map['agency'], '')).strip() if col_map['agency'] else '',
                'agency_contract': str(row.get(col_map['agency_contract'], '')).strip() if col_map['agency_contract'] else '',
                'supplier': str(row.get(col_map['supplier'], '')).strip() if col_map['supplier'] else '',
                'origin': str(row.get(col_map['origin'], '')).strip() if col_map['origin'] else '',
                'size': str(row.get(col_map['size'], '')).strip() if col_map['size'] else '',
                'packing': str(row.get(col_map['packing'], '')).strip() if col_map['packing'] else '',
                'open_qty': safe_float_parse(row.get(col_map['open_qty'])) if col_map['open_qty'] else 0,
                # quantity는 open_qty를 기본값으로 사용
                'quantity': safe_float_parse(row.get(col_map['open_qty'])) if col_map['open_qty'] else 0,
                'doc_qty': safe_float_parse(row.get(col_map['doc_qty'])) if col_map['doc_qty'] else 0,
                'box_qty': safe_float_parse(row.get(col_map['box_qty'])) if col_map['box_qty'] else 0,
                'unit2': str(row.get(col_map['unit2'], '')).strip() if col_map['unit2'] else '',
                'unit_price': safe_float_parse(row.get(col_map['price'])) if col_map['price'] else 0,
                'open_amount': safe_float_parse(row.get(col_map['open_amt'])) if col_map['open_amt'] else 0,
                'doc_amount': safe_float_parse(row.get(col_map['doc_amt'])) if col_map['doc_amt'] else 0,
                'tt_check': str(row.get(col_map['tt'], '')).strip() if col_map['tt'] else '',
                'bank': str(row.get(col_map['bank'], '')).strip() if col_map['bank'] else '',
                'usance': str(row.get(col_map['usance'], '')).strip() if col_map['usance'] else '',
                'at_sight': str(row.get(col_map['at_sight'], '')).strip() if col_map['at_sight'] else '',
                'open_date': safe_date_parse(row.get(col_map['open_date'])) if col_map['open_date'] else None,
                'lc_no': str(row.get(col_map['lc_no'], '')).strip() if col_map['lc_no'] else '',
                'invoice_no': str(row.get(col_map['inv_no'], '')).strip() if col_map['inv_no'] else '',
                'bl_no': str(row.get(col_map['bl_no'], '')).strip() if col_map['bl_no'] else '',
                'lg_no': str(row.get(col_map['lg_no'], '')).strip() if col_map['lg_no'] else '',
                'insurance': str(row.get(col_map['insurance'], '')).strip() if col_map['insurance'] else '',
                'customs_broker_date': safe_date_parse(row.get(col_map['broker_date'])) if col_map['broker_date'] else None,
                'etd': safe_date_parse(row.get(col_map['etd'])) if col_map['etd'] else None,
                'expected_date': safe_date_parse(row.get(col_map['eta'])) if col_map['eta'] else get_kst_today(),
                'arrival_date': safe_date_parse(row.get(col_map['arrival_date'])) if col_map['arrival_date'] else None,
                'warehouse': str(row.get(col_map['wh'], '')).strip() if col_map['wh'] else '',
                'actual_in_qty': safe_float_parse(row.get(col_map['real_in_qty'])) if col_map['real_in_qty'] else 0,
                'destination': str(row.get(col_map['dest'], '')).strip() if col_map['dest'] else '',
                'note': str(row.get(col_map['note'], '')).strip() if col_map['note'] else '',
                'doc_acceptance': safe_date_parse(row.get(col_map['doc_acc'])) if col_map['doc_acc'] else None,
                'acceptance_rate': safe_float_parse(row.get(col_map['acc_rate'])) if col_map['acc_rate'] else 0,
                'maturity_date': safe_date_parse(row.get(col_map['mat_date'])) if col_map['mat_date'] else None,
                'ext_maturity_date': safe_date_parse(row.get(col_map['ext_date'])) if col_map['ext_date'] else None,
                'acceptance_fee': safe_float_parse(row.get(col_map['acc_fee'])) if col_map['acc_fee'] else 0,
                'discount_fee': safe_float_parse(row.get(col_map['dis_fee'])) if col_map['dis_fee'] else 0,
                'payment_date': safe_date_parse(row.get(col_map['pay_date'])) if col_map['pay_date'] else None,
                'payment_amount': safe_float_parse(row.get(col_map['pay_amt'])) if col_map['pay_amt'] else 0,
                'exchange_rate': safe_float_parse(row.get(col_map['ex_rate'])) if col_map['ex_rate'] else 0,
                'balance': safe_float_parse(row.get(col_map['balance'])) if col_map['balance'] else 0,
                'avg_exchange_rate': safe_float_parse(row.get(col_map['avg_ex'])) if col_map['avg_ex'] else 0,
                'status': 'PENDING'
            }
            
            # Nan 문자열 처리 ('nan' 문자열이 DB에 들어가지 않도록)
            for k, v in data.items():
                if isinstance(v, str) and (v.lower() == 'nan' or v.lower() == 'nat'): 
                    data[k] = ''
            
            valid_data.append(data)
            
        except Exception as e:
            errors.append(f"[행 {idx+header_row_idx+2}] 데이터 파싱 오류: {str(e)}")
            
    return valid_data, errors

# ==========================================
# 2. 메인 UI 구성
# ==========================================

st.title("🚢 수입 관리 시스템 (상세)")

tab_status, tab_detail, tab_product = st.tabs(["📊 수입진행상황 (전체조회)", "📝 수입 상세 관리 (입력/수정)", "📦 품목 관리"])

# --- TAB 1: 수입진행상황 (전체조회) ---
with tab_status:
    st.markdown("### 📅 전체 수입 장부 조회")
    
    col_f1, col_f2 = st.columns([1, 4])
    with col_f1:
        view_opt = st.radio("조회 상태", ["전체", "진행중 (PENDING)", "입고완료 (ARRIVED)"], index=0, horizontal=True)
        
    status_filter = 'ALL'
    if "진행중" in view_opt: status_filter = 'PENDING'
    elif "입고완료" in view_opt: status_filter = 'ARRIVED'
    
    df = get_full_schedule_data(status_filter)
    
    if not df.empty:
        # 주요 컬럼만 추려서 보여주기 (너무 많으므로)
        display_cols = [
            'ck_code', 'supplier', 'product_name', 'size', 'quantity', 'unit_price', 'expected_date', 'status', 
            'lc_no', 'bl_no', 'warehouse', 'arrival_date'
        ]
        
        # 컬럼명 한글 매핑
        col_map = {
            'ck_code': 'CK', 'supplier': '수출자', 'product_name': '품명', 'size': '사이즈', 
            'quantity': '수량', 'unit_price': '단가', 'expected_date': 'ETA(입항)', 'status': '상태',
            'lc_no': 'L/C No.', 'bl_no': 'B/L No.', 'warehouse': '창고', 'arrival_date': '입고일'
        }
        
        st.dataframe(
            df[display_cols].rename(columns=col_map),
            use_container_width=True,
            hide_index=True,
            height=600
        )
    else:
        st.info("데이터가 없습니다.")

# --- TAB 2: 수입 상세 관리 (입력/수정) ---
with tab_detail:
    col_list, col_form = st.columns([1, 2])
    
    # [좌측] 리스트 및 선택
    with col_list:
        sub_t1, sub_t2 = st.tabs(["목록 선택", "엑셀 일괄 등록"])
        
        with sub_t1:
            st.subheader("등록 건 목록")
            df_list = get_full_schedule_data('ALL') # 전체 목록 불러오기
            
            # 검색 기능
            search_txt = st.text_input("🔍 검색 (CK, 품명, B/L 등)", key="list_search")
            if not df_list.empty and search_txt:
                mask = df_list.apply(lambda x: x.astype(str).str.contains(search_txt, case=False).any(), axis=1)
                df_list = df_list[mask]
            
            # 신규 등록 버튼
            if st.button("➕ 신규 등록 (빈 양식)", type="primary", use_container_width=True):
                st.session_state['edit_mode'] = 'new'
                st.session_state['selected_data'] = None
                st.rerun()
                
            st.markdown("---")
            
            if not df_list.empty:
                for idx, row in df_list.iterrows():
                    # 카드 형태로 표시
                    label = f"**[{row['ck_code'] or 'NO-CK'}]** {row['product_name']}"
                    sub = f"{row['supplier'] or '미지정'} | ETA: {row['expected_date']} | {row['status']}"
                    
                    with st.container(border=True):
                        st.markdown(label)
                        st.caption(sub)
                        if st.button("상세/수정", key=f"sel_{row['id']}", use_container_width=True):
                            st.session_state['edit_mode'] = 'edit'
                            st.session_state['selected_data'] = row.to_dict()
                            st.rerun()
            else:
                st.info("데이터가 없습니다.")
        
        with sub_t2:
            st.subheader("엑셀 파일 업로드")
            st.caption("※ '수입' 탭 양식의 CSV 파일을 업로드하세요. '품명'이 시스템에 등록되어 있어야 합니다.")
            up_file = st.file_uploader("파일 선택", type=['csv'])
            if up_file:
                if st.button("분석 및 등록 시작", use_container_width=True):
                    try:
                        # CSV 파일 읽기
                        df_up = pd.read_csv(up_file)
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
        
        title_prefix = "수정" if edit_mode == 'edit' else "신규 등록"
        st.subheader(f"📝 상세 정보 {title_prefix}")
        
        if edit_mode == 'edit' and not data:
            st.info("좌측 목록에서 항목을 선택해주세요.")
        else:
            with st.form("detail_form"):
                # 1. 기본 식별 정보
                st.markdown("<div class='form-header'>기본 식별 정보</div>", unsafe_allow_html=True)
                c1, c2, c3, c4 = st.columns(4)
                ck_code = c1.text_input("CK 관리번호", value=data.get('ck_code', ''))
                global_code = c2.text_input("글로벌 번호", value=data.get('global_code', ''))
                doojin_code = c3.text_input("두진 번호", value=data.get('doojin_code', ''))
                
                # 품목 선택 (DB 연동)
                p_df = get_products_df()
                p_opts = {row['ID']: f"[{row['카테고리']}] {row['품목명']} ({row['품목코드']})" for _, row in p_df.iterrows()}
                def_pid = data.get('product_id')
                if def_pid not in p_opts: def_pid = None
                
                # index 찾기
                opt_keys = list(p_opts.keys())
                sel_idx = opt_keys.index(def_pid) if def_pid in opt_keys else 0
                
                sel_pid = c4.selectbox("품목 (필수)", options=opt_keys, format_func=lambda x: p_opts[x], index=sel_idx)

                # 2. 계약 및 물품 정보
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
                quantity = c1.number_input("오픈 수량", value=float(data.get('quantity') or 0.0)) # 기본 수량 컬럼 사용
                doc_qty = c2.number_input("서류 수량", value=float(data.get('doc_qty') or 0.0))
                box_qty = c3.number_input("박스 수량", value=float(data.get('box_qty') or 0.0))
                
                # 금액 자동 계산 (단순 참고용)
                est_amt = quantity * unit_price
                open_amount = c4.number_input("오픈 금액", value=float(data.get('open_amount') or est_amt))

                # 3. L/C 및 서류 정보
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

                # 4. 일정 및 물류 정보
                st.markdown("<div class='form-header'>일정 및 물류 정보</div>", unsafe_allow_html=True)
                c1, c2, c3, c4 = st.columns(4)
                etd = c1.date_input("ETD (출항)", value=data.get('etd'))
                # expected_date를 ETA로 사용
                eta = c2.date_input("ETA (입항/예정일)", value=data.get('expected_date') or get_kst_today())
                arrival_date = c3.date_input("실 입고일", value=data.get('arrival_date'))
                customs_broker_date = c4.date_input("관세사 전달일", value=data.get('customs_broker_date'))

                c1, c2, c3 = st.columns(3)
                warehouse = c1.text_input("창고", value=data.get('warehouse', ''))
                destination = c2.text_input("착지", value=data.get('destination', ''))
                actual_in_qty = c3.number_input("실 입고 수량", value=float(data.get('actual_in_qty') or 0.0))

                # 5. 결제 정보
                st.markdown("<div class='form-header'>결제 정보</div>", unsafe_allow_html=True)
                c1, c2, c3, c4 = st.columns(4)
                doc_acceptance = c1.date_input("서류 인수일", value=data.get('doc_acceptance'))
                maturity_date = c2.date_input("만기일", value=data.get('maturity_date'))
                payment_date = c3.date_input("결제일", value=data.get('payment_date'))
                payment_amount = c4.number_input("결제 금액", value=float(data.get('payment_amount') or 0.0))

                # 비고 및 상태
                st.markdown("<div class='form-header'>기타</div>", unsafe_allow_html=True)
                note = st.text_area("비고", value=data.get('note', ''))
                status = st.selectbox("진행 상태", ["PENDING", "ARRIVED", "CANCELED"], index=["PENDING", "ARRIVED", "CANCELED"].index(data.get('status', 'PENDING')))

                # 저장 버튼
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
                            'payment_amount': payment_amount, 'note': note, 'status': status
                        }
                        
                        sid = data.get('id') if edit_mode == 'edit' else None
                        succ, msg = save_full_schedule(save_data, sid)
                        
                        if succ:
                            st.success(msg)
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"저장 실패: {msg}")
                
                with c_del:
                    if edit_mode == 'edit':
                        if st.form_submit_button("🗑️ 삭제"):
                            delete_schedule(data['id'])
                            st.session_state['edit_mode'] = 'new'
                            st.session_state['selected_data'] = None
                            st.rerun()

# --- TAB 3: 품목 관리 (DB) ---
with tab_product:
    st.markdown("### 📦 시스템 품목 관리")
    
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