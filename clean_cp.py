import pandas as pd

def clean_and_merge_csv():
    selected_columns = [
        "url", "post_id", "content", "date_posted", "num_comments", "num_shares", 
        "num_likes_type", "original_post", "attachments", "video_view_count", "likes", 
        "count_reactions_type", "post_image", "post_type", "page_reviews_score"
    ]
    
    all_data = []
    
    # Đọc và làm sạch từng file
    for i in range(1, 25):
        file_name = f"cp_{i}.csv"
        try:
            df = pd.read_csv(file_name, usecols=selected_columns, dtype=str)
            df = df.dropna(subset=["post_id"])  # Loại bỏ dòng có post_id trống
            all_data.append(df)
        except Exception as e:
            print(f"Lỗi khi xử lý {file_name}: {e}")
    
    # Gộp tất cả file lại thành một DataFrame
    merged_df = pd.concat(all_data, ignore_index=True)
    
    # Gộp các dòng có cùng post_id bằng cách giữ giá trị đầu tiên cho mỗi cột
    merged_df = merged_df.groupby("post_id", as_index=False).first()
    
    # Chuyển đổi date_posted sang datetime và sắp xếp từ mới nhất đến trễ nhất
    merged_df["date_posted"] = pd.to_datetime(merged_df["date_posted"], errors='coerce')
    merged_df = merged_df.sort_values(by="date_posted", ascending=False)
    
    # Xuất ra file mới
    merged_df.to_csv("cp_1_25.csv", index=False)
    
    # Print số lượng dòng
    print(f"Đã tạo file cp_1_25.csv thành công! Tổng số dòng: {len(merged_df)}")

if __name__ == "__main__":
    clean_and_merge_csv()

    # https://www.facebook.com/thongtinchinhphu
#     Tháng 7/2024



#t9 -> d 04:31:59
#t8 -> d 05:33:59
#t7 -> d 07:38:54

#cp -> d 05:30:56

#t10 -> d 04:29:17
#t11 -> d 05:03:36



# còn 10 11 16 17 18 tháng 12






# r -> crawing
# d-> downloaded to vscode and ready to continue
# re -> wait to download to vscode


