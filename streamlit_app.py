import streamlit as st
import pandas as pd
import logic
import data_fetcher
import concurrent.futures

# --- CẤU HÌNH ---
st.set_page_config(
    page_title="SIÊU GÀ APP - PRO",
    page_icon="🐔",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS FIX LỖI FONT & GIAO DIỆN + RESPONSIVE ---
st.markdown("""
<style>
    /* Fix lỗi font menu bị chìm trong dark mode */
    .stTabs [data-baseweb="tab-list"] { gap: 4px; }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background-color: #e0e0e0;
        border-radius: 5px 5px 0 0;
        padding: 10px;
        color: #000000 !important; /* Ép màu chữ đen */
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        background-color: #ff4b4b !important;
        color: #ffffff !important;
        border-top: 2px solid #ff4b4b;
    }
    /* Căn giữa ô bảng */
    .stDataFrame td { vertical-align: middle !important; }
    
    /* === RESPONSIVE TABLE WRAPPER === */
    .table-wrapper {
        overflow-x: auto;
        -webkit-overflow-scrolling: touch;
        margin: 10px 0;
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
    
    /* === RESPONSIVE TABLE STYLES === */
    .responsive-table {
        border-collapse: collapse;
        width: 100%;
        font-size: 12px;
        min-width: 600px; /* Minimum width để table không bị vỡ */
    }
    
    .responsive-table th {
        padding: 6px 4px;
        border: 1px solid #34495e;
        background-color: #2c3e50;
        color: white;
        text-align: center;
        white-space: nowrap;
        position: sticky;
        top: 0;
        z-index: 10;
        font-size: 11px;
    }
    
    .responsive-table td {
        padding: 5px 3px;
        border: 1px solid #dee2e6;
        text-align: center;
    }
    
    /* Sticky first 2 columns on desktop */
    @media (min-width: 768px) {
        .responsive-table th:nth-child(1),
        .responsive-table td:nth-child(1) {
            position: sticky;
            left: 0;
            z-index: 5;
            background-color: #2c3e50;
        }
        
        .responsive-table td:nth-child(1) {
            background-color: inherit;
            font-weight: bold;
        }
        
        .responsive-table th:nth-child(2),
        .responsive-table td:nth-child(2) {
            position: sticky;
            left: 80px;
            z-index: 5;
        }
    }
    
    /* === MOBILE RESPONSIVE (< 768px) === */
    @media (max-width: 767px) {
        .responsive-table {
            font-size: 11px;
            min-width: 100%;
        }
        
        .responsive-table th {
            padding: 4px 3px;
            font-size: 10px;
        }
        
        .responsive-table td {
            padding: 4px 2px;
            font-size: 11px;
        }
        
        /* Giảm width cho cột ngày và giải */
        .responsive-table th:nth-child(1),
        .responsive-table td:nth-child(1) {
            min-width: 70px;
            font-size: 10px;
        }
        
        .responsive-table th:nth-child(2),
        .responsive-table td:nth-child(2) {
            min-width: 50px;
        }
        
        .responsive-table th:nth-child(3),
        .responsive-table td:nth-child(3) {
            min-width: 120px;
            font-size: 9px;
        }
        
        .responsive-table th:nth-child(4),
        .responsive-table td:nth-child(4) {
            min-width: 40px;
        }
        
        /* Cột N1, N2, N3... */
        .responsive-table th:nth-child(n+5),
        .responsive-table td:nth-child(n+5) {
            min-width: 32px;
            padding: 3px 2px;
        }
    }
    
    /* === EXTRA SMALL MOBILE (< 480px) === */
    @media (max-width: 479px) {
        .responsive-table {
            font-size: 10px;
        }
        
        .responsive-table th {
            padding: 3px 2px;
            font-size: 9px;
        }
        
        .responsive-table td {
            padding: 3px 1px;
            font-size: 10px;
        }
        
        .responsive-table th:nth-child(1),
        .responsive-table td:nth-child(1) {
            min-width: 60px;
            font-size: 9px;
        }
        
        .responsive-table th:nth-child(2),
        .responsive-table td:nth-child(2) {
            min-width: 45px;
        }
        
        .responsive-table th:nth-child(3),
        .responsive-table td:nth-child(3) {
            min-width: 100px;
            font-size: 8px;
        }
        
        .responsive-table th:nth-child(n+5),
        .responsive-table td:nth-child(n+5) {
            min-width: 30px;
            padding: 2px 1px;
        }
    }
    
    /* Scroll indicator hint */
    .scroll-hint {
        text-align: center;
        color: #7f8c8d;
        font-size: 12px;
        margin-top: 5px;
        display: none;
    }
    
    @media (max-width: 767px) {
        .scroll-hint {
            display: block;
        }
    }
</style>
""", unsafe_allow_html=True)

# --- QUẢN LÝ DỮ LIỆU ---
@st.cache_data(ttl=1800)
def get_master_data(num_days):
    # Tải song song tất cả các nguồn
    with concurrent.futures.ThreadPoolExecutor() as executor:
        f_dt = executor.submit(data_fetcher.fetch_dien_toan, num_days)
        f_tt = executor.submit(data_fetcher.fetch_than_tai, num_days)
        f_mb = executor.submit(data_fetcher.fetch_xsmb_group, num_days)
        
        dt = f_dt.result()
        tt = f_tt.result()
        mb_db, mb_g1 = f_mb.result()

    # Xử lý khớp ngày (Quan trọng để không bị lệch)
    df_dt = pd.DataFrame(dt)
    df_tt = pd.DataFrame(tt)
    
    xsmb_rows = []
    limit = min(len(dt), len(mb_db), len(mb_g1))
    for i in range(limit):
        xsmb_rows.append({
            "date": dt[i]["date"], # Dùng ngày của Điện Toán làm chuẩn
            "xsmb_full": mb_db[i],
            "xsmb_2so": mb_db[i][-2:],
            "g1_full": mb_g1[i],
            "g1_2so": mb_g1[i][-2:]
        })
    df_xsmb = pd.DataFrame(xsmb_rows)

    # Gộp thành bảng tổng (Master Table)
    if not df_dt.empty and not df_xsmb.empty:
        df = pd.merge(df_dt, df_tt, on="date", how="left")
        df = pd.merge(df, df_xsmb, on="date", how="left")
        return df
    return pd.DataFrame()

# --- SIDEBAR ---
with st.sidebar:
    st.title("🐔 SIÊU GÀ TOOL")
    st.caption("Version: Matrix View")
    days_fetch = st.number_input("Số ngày tải:", 30, 365, 60, step=10)
    days_show = st.slider("Hiển thị:", 10, 100, 20)
    if st.button("🔄 Tải lại dữ liệu", type="primary"):
        st.cache_data.clear()
        st.rerun()

# --- LOAD DATA ---
try:
    with st.spinner("🚀 Đang tải dữ liệu đa luồng..."):
        df_full = get_master_data(days_fetch)
        if df_full.empty:
            st.error("Không có dữ liệu. Kiểm tra kết nối mạng.")
            st.stop()
except Exception as e:
    st.error(f"Lỗi: {e}")
    st.stop()

df_show = df_full.head(days_show).copy()

# === 🎯 DÀN NUÔI (MATRIX) ===
st.title("🎯 DÀN NUÔI (MATRIX)")
st.divider()

# Row 1: Nguồn và Miền
c1, c2 = st.columns([1, 1])
src_mode = c1.selectbox("Nguồn:", ["Thần Tài", "Điện Toán"])
region = c2.selectbox("Miền:", ["Miền Bắc", "Miền Nam", "Miền Trung"])

# Row 2: Thứ, Đài, Giải (cho Miền Nam/Trung) hoặc So với (cho Miền Bắc)
if region == "Miền Bắc":
    # Miền Bắc: Giữ nguyên logic cũ
    c3, c4, c5 = st.columns([1.5, 1.5, 1.5])
    comp_mode = c3.selectbox("So với:", ["XSMB (ĐB)", "Giải Nhất"])
    check_range = c4.slider("Khung nuôi (ngày):", 1, 20, 7)
    backtest_mode = c5.selectbox("Backtest:", ["Hiện tại", "Lùi 1 ngày", "Lùi 2 ngày", "Lùi 3 ngày", "Lùi 4 ngày", "Lùi 5 ngày"])
    
    # Xác định cột so sánh
    col_comp = "xsmb_2so" if "ĐB" in comp_mode else "g1_2so"
    selected_station = None
    
else:
    # Miền Nam/Trung: Hệ thống mới với Thứ/Đài/Giải
    c3, c4, c5, c6, c7 = st.columns([1, 1.5, 1, 1.5, 1.5])
    
    # Dropdown Thứ
    weekdays = ["Chủ Nhật", "Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7"]
    selected_day = c3.selectbox("Thứ:", weekdays)
    
    # Dropdown Đài (dựa trên Miền và Thứ)
    stations = data_fetcher.get_stations_by_day(region, selected_day)
    if not stations:
        st.error(f"⚠️ Không có đài nào mở thưởng vào {selected_day} ở {region}")
        st.stop()
    
    # Thêm tùy chọn "Tất cả" vào đầu danh sách
    station_options = ["Tất cả"] + stations
    selected_station = c4.selectbox("Đài:", station_options)
    
    # Dropdown Giải
    prize_mode = c5.selectbox("Giải:", ["Đặc Biệt", "Giải Nhất"])
    
    # Khung nuôi và Backtest
    check_range = c6.slider("Khung nuôi (ngày):", 1, 20, 7)
    backtest_mode = c7.selectbox("Backtest:", ["Hiện tại", "Lùi 1 ngày", "Lùi 2 ngày", "Lùi 3 ngày", "Lùi 4 ngày", "Lùi 5 ngày"])
    
    # Xác định cột so sánh
    col_comp = "db_2so" if prize_mode == "Đặc Biệt" else "g1_2so"

# Tự động phân tích
backtest_offset = 0
if backtest_mode != "Hiện tại":
    backtest_offset = int(backtest_mode.split()[1])

if backtest_offset > 0:
    st.info(f"🔍 Backtest: Từ {backtest_offset} ngày trước")

# === LOAD DỮ LIỆU ===
if region == "Miền Bắc":
    # Sử dụng dữ liệu df_full đã load sẵn từ trước
    df_region = df_full
else:
    # Load dữ liệu từ API
    if selected_station == "Tất cả":
        # Load tất cả các đài trong ngày
        with st.spinner(f"🔄 Đang tải dữ liệu tất cả các đài ({', '.join(stations)})..."):
            all_station_data = []
            # Tải song song
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_station = {executor.submit(data_fetcher.fetch_station_data, s, days_fetch): s for s in stations}
                for future in concurrent.futures.as_completed(future_to_station):
                    station_name = future_to_station[future]
                    try:
                        data = future.result()
                        # Thêm tên đài vào mỗi dòng dữ liệu
                        for item in data:
                            item['station'] = station_name
                        all_station_data.extend(data)
                    except Exception as exc:
                        st.error(f"Lỗi tải đài {station_name}: {exc}")
            
            if not all_station_data:
                st.error("⚠️ Không thể tải dữ liệu")
                st.stop()
            
            # Chuyển đổi sang DataFrame và gom nhóm theo ngày
            df_temp = pd.DataFrame(all_station_data)
            
            # Gom nhóm theo ngày, tạo list các kết quả
            # Chúng ta cần giữ lại thông tin đài nào có kết quả nào
            # Cấu trúc mới: date | results (list of dicts: {station, val})
            
            grouped_data = []
            for date, group in df_temp.groupby('date'):
                # Gom tất cả kết quả của ngày này lại
                day_results = []
                for _, row in group.iterrows():
                    val = row.get(col_comp, "")
                    if val:
                        day_results.append({'station': row['station'], 'val': val})
                
                if day_results:
                    grouped_data.append({'date': date, 'results': day_results})
            
            df_region = pd.DataFrame(grouped_data).sort_values('date', ascending=False)
            
    else:
        # Load dữ liệu cho đài đã chọn
        with st.spinner(f"🔄 Đang tải dữ liệu {selected_station}..."):
            station_data = data_fetcher.fetch_station_data(selected_station, total_days=days_fetch)
            
            if not station_data:
                st.error(f"⚠️ Không thể tải dữ liệu cho {selected_station}")
                st.stop()
            
            # Chuyển đổi sang DataFrame
            # Chuẩn hóa cấu trúc để giống với "Tất cả" (mỗi ngày 1 list kết quả)
            df_temp = pd.DataFrame(station_data)
            df_temp['results'] = df_temp.apply(lambda x: [{'station': selected_station, 'val': x.get(col_comp, "")}], axis=1)
            df_region = df_temp[['date', 'results']]


all_days_data = []
start_idx = backtest_offset
end_idx = min(backtest_offset + 20, len(df_region))

# Tạo lookup dictionary cho df_full để tra cứu nhanh theo ngày
df_full_lookup = df_full.set_index('date') if not df_full.empty else pd.DataFrame()

for i in range(start_idx, end_idx):
    row = df_region.iloc[i]
    date_val = row['date']
    
    # Xác định dòng dữ liệu nguồn (Source Row)
    # Nếu là Miền Bắc thì chính là row hiện tại
    # Nếu là Miền Nam/Trung thì phải tìm ngày tương ứng trong df_full
    row_src = None
    if region == "Miền Bắc":
        row_src = row
    else:
        if date_val in df_full_lookup.index:
            row_src = df_full_lookup.loc[date_val]
            # Xử lý trường hợp trùng ngày (nếu có)
            if isinstance(row_src, pd.DataFrame):
                row_src = row_src.iloc[0]
    
    if row_src is None:
        continue

    src_str = ""
    if src_mode == "Thần Tài": 
        src_str = str(row_src.get('tt_number', ''))
    elif src_mode == "Điện Toán": 
        val = row_src.get('dt_numbers', [])
        if isinstance(val, list):
             src_str = "".join(val)
        else:
             src_str = str(val) if pd.notna(val) else ""
    
    if not src_str or src_str == "nan": 
        continue
    
    # ... (previous code)
    
    digits = set(src_str)
    combos = sorted({a+b for a in digits for b in digits})
    
    # Store results for this date (for comparison later)
    # If Miền Bắc: result is in row[col_comp]
    # If Miền Nam/Trung: result is in row['results'] (list of dicts)
    
    date_results = []
    if region == "Miền Bắc":
        val = str(row.get(col_comp, ""))
        if val and val != "nan":
            date_results.append({'station': 'XSMB', 'val': val})
    else:
        # row['results'] is already a list of dicts {station, val}
        res_list = row.get('results', [])
        if isinstance(res_list, list):
            date_results = res_list
            
    all_days_data.append({
        'date': row['date'], 
        'source': src_str, 
        'combos': combos, 
        'index': i,
        'results': date_results
    })

if not all_days_data:
    st.warning("⚠️ Không có dữ liệu")
else:
    st.markdown("### 📋 Bảng Theo Dõi")
    
    # Wrapper div cho responsive
    table_html = "<div class='table-wrapper'>"
    table_html += "<table class='responsive-table'><tr>"
    table_html += "<th>Ngày</th>"
    table_html += "<th>Giải</th>"
    table_html += "<th>Dàn nhị hợp</th>"
    table_html += "<th>Mức</th>"
    
    num_days = len(all_days_data)
    for k in range(1, num_days + 1):
        table_html += f"<th>N{k}</th>"
    table_html += "</tr>"
    
    for row_idx, day_data in enumerate(all_days_data):
        date, source, combos, i, day_results = day_data['date'], day_data['source'], day_data['combos'], day_data['index'], day_data['results']
        dan_str = " ".join(combos[:15]) + ("..." if len(combos) > 15 else "")
        row_bg = "#f8f9fa" if row_idx % 2 == 0 else "#ffffff"
        table_html += f"<tr style='background-color: {row_bg};'><td style='font-weight: bold; color: #2c3e50;'>{date}</td>"
        table_html += f"<td style='color: #495057;'>{source}</td>"
        table_html += f"<td style='font-size: 11px; color: #495057;'>{dan_str}</td>"
        table_html += f"<td style='font-weight: 600; color: #2c3e50;'>{len(combos)}</td>"
        
        # Check results for next N days
        for k in range(1, num_days + 1):
            check_idx = i - k
            cell_content = ""
            cell_style = ""
            
            if check_idx >= 0:
                # Get results for the check date
                # We need to find the data for date at check_idx
                # Since all_days_data is built from df_region.iloc[start:end], 
                # and we iterate i from start to end.
                # Wait, check_idx is index in df_region.
                # We need to access df_region to get results for past days?
                # No, the table columns are N1, N2... meaning "Result of Day + 1", "Result of Day + 2" relative to current row?
                # Usually "Nuôi" means we play this set for next N days.
                # So we check results of date + 1, date + 2...
                # But here the loop is iterating backwards or forwards?
                # df_region is sorted desc? No, usually asc or desc.
                # Let's check df_region sort order. Usually it's desc (newest first).
                # If i is current day, i-1 is tomorrow (if desc).
                
                # Let's assume df_region is sorted DESC (Newest top).
                # Then i is current. i-1 is NEWER (Tomorrow). i+1 is OLDER (Yesterday).
                # "Nuôi" means we predict for FUTURE.
                # So we look at i-1, i-2...
                
                if check_idx < len(df_region): # Should be valid index
                     # Get results from df_region directly to be safe
                    check_row = df_region.iloc[check_idx]
                    
                    # Extract results
                    check_results = []
                    if region == "Miền Bắc":
                        val = str(check_row.get(col_comp, ""))
                        if val and val != "nan":
                            check_results.append({'station': 'XSMB', 'val': val})
                    else:
                        res_list = check_row.get('results', [])
                        if isinstance(res_list, list):
                            check_results = res_list
                    
                    # Compare
                    hit_stations = []
                    for res in check_results:
                         if res['val'] in combos:
                             hit_stations.append(res)
                    
                    if hit_stations:
                        cell_style = "background-color: #d4edda; color: #155724; font-weight: bold;"
                        # Show matched value and station
                        display_strs = []
                        for h in hit_stations:
                            st_name = h['station']
                            # Abbreviate station name if needed or just show
                            if st_name == "XSMB": st_name = ""
                            else: st_name = f" ({st_name})"
                            display_strs.append(f"{h['val']}{st_name}")
                        cell_content = "<br>".join(display_strs)
                    else:
                        # Show first result or empty
                        if check_results:
                             # Just show the first value for reference, or all?
                             # Showing all might be too much. Let's show first 1-2.
                             display_strs = []
                             for res in check_results[:2]:
                                 st_name = res['station']
                                 if st_name == "XSMB": st_name = ""
                                 else: st_name = f" ({st_name})"
                                 display_strs.append(f"{res['val']}{st_name}")
                             cell_content = "<br>".join(display_strs)
                             if len(check_results) > 2: cell_content += "..."
                        else:
                             cell_content = "-"

            table_html += f"<td style='{cell_style}; font-size: 11px; min-width: 60px;'>{cell_content}</td>"
        table_html += "</tr>"
    
    table_html += "</table></div>"
    st.markdown(table_html, unsafe_allow_html=True)
        
    st.markdown("---")
    st.subheader("📊 Thống kê")
    total_days, total_checks, total_hits = len(all_days_data), 0, 0
    for row_idx, day_data in enumerate(all_days_data):
        combos, i = day_data['combos'], day_data['index']
        for k in range(1, row_idx + 2):
            idx = i - k
            # Chỉ tính nếu idx >= backtest_offset (không tính "tương lai")
            if idx >= 0 and idx >= backtest_offset:
                total_checks += 1
                
                # Check hit
                check_row = df_region.iloc[idx]
                is_hit = False
                
                if region == "Miền Bắc":
                    val = str(check_row.get(col_comp, ""))
                    if val and val != "nan" and val in combos:
                        is_hit = True
                else:
                    res_list = check_row.get('results', [])
                    if isinstance(res_list, list):
                        for res in res_list:
                            if res['val'] in combos:
                                is_hit = True
                                break
                
                if is_hit:
                    total_hits += 1
    
    hit_rate = round(total_hits / total_checks * 100, 1) if total_checks > 0 else 0
    col_s1, col_s2, col_s3, col_s4 = st.columns(4)
    col_s1.metric("Tổng ngày", total_days)
    col_s2.metric("Tổng kiểm tra", total_checks)
    col_s3.metric("Đã trúng", total_hits)
    col_s4.metric("Tỷ lệ", f"{hit_rate}%")
    
    # === TỔNG HỢP DÀN CHƯA RA ===
    st.markdown("---")
    st.subheader("🎯 Tổng hợp Dàn Chưa Ra")
    st.caption("Các dàn nhị hợp chưa ra (chưa trúng số nào)")
    
    # Thu thập dữ liệu theo ngày - chỉ những dàn HOÀN TOÀN chưa ra
    from datetime import datetime
    pending_by_date = []
    
    for row_idx, day_data in enumerate(all_days_data):
        combos = day_data['combos']
        date = day_data['date']
        i = day_data['index']
        num_cols_this_row = row_idx + 1
        hit_numbers = set()
        
        # Kiểm tra xem có số nào trong dàn đã trúng chưa (chỉ xét dữ liệu lịch sử)
        # Kiểm tra xem có số nào trong dàn đã trúng chưa (chỉ xét dữ liệu lịch sử)
        for k in range(1, num_cols_this_row + 1):
            idx = i - k
            if idx >= 0 and idx >= backtest_offset:
                check_row = df_region.iloc[idx]
                
                if region == "Miền Bắc":
                    val_res = str(check_row.get(col_comp, ""))
                    if val_res and val_res != "nan" and val_res in combos:
                        hit_numbers.add(val_res)
                else:
                    res_list = check_row.get('results', [])
                    if isinstance(res_list, list):
                        for res in res_list:
                            if res['val'] in combos:
                                hit_numbers.add(res['val'])
        
        # Nếu CHƯA có số nào trúng (hit_numbers rỗng) thì dàn này chưa ra
        if not hit_numbers:
            # Parse date để lấy thứ
            try:
                date_obj = datetime.strptime(date, "%d/%m/%Y")
                weekday_names = ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ Nhật"]
                weekday = weekday_names[date_obj.weekday()]
            except:
                weekday = ""
            
            pending_by_date.append({
                'Ngày': f"{weekday} {date}" if weekday else date,
                'Dàn nhị hợp': ', '.join(sorted(combos)),
                'Số lượng': len(combos),
                'combos': combos  # Giữ lại để phân tích tần suất
            })
    
    if pending_by_date:
        # Hiển thị bảng theo ngày
        df_display = pd.DataFrame([{k: v for k, v in item.items() if k != 'combos'} for item in pending_by_date])
        st.dataframe(df_display, use_container_width=True, hide_index=True)
        
        # Phân tích tần suất các số trong các dàn chưa ra
        st.markdown("---")
        st.markdown("**📊 Mức số trong các dàn chưa ra:**")
        st.caption("Đếm số lần xuất hiện của mỗi số trong tất cả các dàn chưa ra")
        
        # Đếm tần suất
        from collections import defaultdict
        number_frequency = defaultdict(int)
        for item in pending_by_date:
            for num in item['combos']:
                number_frequency[num] += 1
        
        # Nhóm theo mức (bao gồm mức 0)
        level_groups = defaultdict(list)
        for num, freq in number_frequency.items():
            level_groups[freq].append(num)
        
        # Tìm tất cả số từ 00-99 và thêm mức 0
        all_possible_numbers = {f"{i:02d}" for i in range(100)}
        numbers_in_pending = set(number_frequency.keys())
        level_0_numbers = sorted(all_possible_numbers - numbers_in_pending)
        
        if level_0_numbers:
            level_groups[0] = level_0_numbers
        
        # Hiển thị theo mức giảm dần
        for freq in sorted(level_groups.keys(), reverse=True):
            nums = sorted(level_groups[freq])
            st.write(f"**Mức {freq}** ({len(nums)} số): {', '.join(nums)}")
        
        # Thống kê tổng quan
        st.markdown("---")
        total_days_pending = len(pending_by_date)
        total_unique_numbers = len(number_frequency)
        col_p1, col_p2 = st.columns(2)
        col_p1.metric("Số ngày có dàn chưa ra", total_days_pending)
        col_p2.metric("Tổng số unique trong các dàn", total_unique_numbers)
    else:
        st.success("✅ Tất cả các dàn đều đã ra (có ít nhất 1 số trúng)!")
    
    # === PHÂN TÍCH CHU KỲ & NHẬN ĐỊNH ===
    st.markdown("---")
    st.subheader("🔮 Phân tích Chu kỳ & Nhận định")
    st.caption("Dựa trên dữ liệu bảng theo dõi")
    
    # Thu thập dữ liệu chu kỳ cho mỗi dàn
    cycle_analysis = []
    
    for row_idx, day_data in enumerate(all_days_data):
        combos = day_data['combos']
        date = day_data['date']
        i = day_data['index']
        
        # Phân tích dữ liệu từ bảng theo dõi
        num_cols_this_row = row_idx + 1
        hits = []  # Vị trí các lần trúng (1, 2, 3...)
        misses = []  # Vị trí các lần không trúng
        
        for k in range(1, num_cols_this_row + 1):
            idx = i - k
            if idx >= 0 and idx >= backtest_offset:
                val_res = df_region.iloc[idx][col_comp]
                if val_res in combos:
                    hits.append(k)
                else:
                    misses.append(k)
        
        # Tính toán chu kỳ và nhận định
        total_checks = len(hits) + len(misses)
        hit_count = len(hits)
        miss_count = len(misses)
        
        if total_checks == 0:
            status = "🆕 Mới tạo - Chưa có dữ liệu"
            avg_cycle_display = "N/A"
            last_hit_display = "N/A"
            priority = 2
            overdue = 0
        elif hit_count == 0:
            # Chưa ra lần nào
            status = f"🔥 Chưa ra ({total_checks} ngày kiểm tra) - Ưu tiên cao"
            avg_cycle_display = "Chưa ra"
            last_hit_display = "Chưa bao giờ"
            priority = 0
            overdue = total_checks
        else:
            # Đã ra ít nhất 1 lần
            # Tính chu kỳ giữa các lần trúng
            if len(hits) > 1:
                cycles = [hits[j-1] - hits[j] for j in range(1, len(hits))]
                avg_cycle = round(sum(cycles) / len(cycles), 1)
            else:
                avg_cycle = hits[0]
            
            avg_cycle_display = f"{avg_cycle} ngày"
            last_hit_display = f"N{hits[0]}"
            
            # Nhận định dựa trên chu kỳ
            days_since_last = hits[0] - 1  # Số ngày từ lần trúng cuối
            
            if days_since_last == 0:
                status = "✅ Vừa trúng hôm qua"
                priority = 2
                overdue = 0
            elif days_since_last < avg_cycle:
                remaining = round(avg_cycle - days_since_last)
                status = f"⏳ Trong chu kỳ (còn ~{remaining} ngày)"
                priority = 2
                overdue = 0
            else:
                overdue_days = days_since_last - avg_cycle
                if overdue_days > avg_cycle * 0.5:
                    status = f"⚠️ Quá chu kỳ {round(overdue_days)} ngày - Ưu tiên cao"
                    priority = 1
                    overdue = overdue_days
                else:
                    status = f"📍 Quá chu kỳ {round(overdue_days)} ngày"
                    priority = 1
                    overdue = overdue_days
        
        cycle_analysis.append({
            'Ngày': date,
            'Dàn': ', '.join(sorted(combos)),
            'Chu kỳ TB': avg_cycle_display,
            'Lần cuối ra': last_hit_display,
            'Đã kiểm tra': total_checks,
            'Trúng/Trượt': f"{hit_count}/{miss_count}",
            'Nhận định': status,
            # Thêm các trường ẩn để sắp xếp
            '_sort_priority': priority,
            '_overdue_days': overdue,
            '_total_checks': total_checks
        })
    
    if cycle_analysis:
        # Sắp xếp: Ưu tiên chưa ra (nhiều ngày nhất), sau đó quá chu kỳ nhiều nhất, sau đó trong chu kỳ
        cycle_analysis.sort(key=lambda x: (x['_sort_priority'], -x['_overdue_days'], -x['_total_checks']))
        
        # Loại bỏ các trường ẩn trước khi hiển thị
        cycle_analysis_display = [{k: v for k, v in item.items() if not k.startswith('_')} for item in cycle_analysis]
        
        df_cycle = pd.DataFrame(cycle_analysis_display)
        st.dataframe(df_cycle, use_container_width=True, hide_index=True)
        
        # Gợi ý ưu tiên
        st.markdown("---")
        st.markdown("**💡 Gợi ý ưu tiên theo dõi:**")
        
        # Lọc các dàn ưu tiên cao
        priority_sets = [item for item in cycle_analysis if "Ưu tiên cao" in item['Nhận định'] or "Chưa ra lần nào" in item['Nhận định']]
        
        if priority_sets:
            st.info(f"Có **{len(priority_sets)}** dàn cần ưu tiên theo dõi (quá hạn hoặc chưa ra lần nào)")
            
            # Hiển thị danh sách dàn ưu tiên
            st.markdown("**📋 Danh sách dàn ưu tiên:**")
            for idx, item in enumerate(priority_sets, 1):
                st.write(f"{idx}. **{item['Ngày']}**: {item['Dàn']} - _{item['Nhận định']}_")
            
            # Phân tích mức số trong các dàn ưu tiên
            st.markdown("---")
            st.markdown("**📊 Mức số trong các dàn ưu tiên:**")
            
            from collections import defaultdict
            priority_number_freq = defaultdict(int)
            
            # Đếm tần suất từ dàn gốc (không phải string đã format)
            for row_idx, day_data in enumerate(all_days_data):
                date = day_data['date']
                combos = day_data['combos']
                
                # Kiểm tra xem dàn này có trong danh sách ưu tiên không
                is_priority = any(p['Ngày'] == date for p in priority_sets)
                
                if is_priority:
                    for num in combos:
                            priority_number_freq[num] += 1
            
            # Nhóm theo mức (bao gồm mức 0)
            level_groups_priority = defaultdict(list)
            for num, freq in priority_number_freq.items():
                level_groups_priority[freq].append(num)
            
            # Tìm tất cả số từ 00-99 và thêm mức 0
            all_possible_numbers = {f"{i:02d}" for i in range(100)}
            numbers_in_priority = set(priority_number_freq.keys())
            level_0_numbers = sorted(all_possible_numbers - numbers_in_priority)
            
            if level_0_numbers:
                level_groups_priority[0] = level_0_numbers
            
            # Hiển thị theo mức giảm dần
            for freq in sorted(level_groups_priority.keys(), reverse=True):
                nums = sorted(level_groups_priority[freq])
                st.write(f"**Mức {freq}** ({len(nums)} số): {', '.join(nums)}")
        else:
            st.success("Tất cả các dàn đang trong chu kỳ bình thường")
    else:
        pass  # Không có dữ liệu để phân tích chu kỳ

