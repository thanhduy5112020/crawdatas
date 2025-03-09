import pandas as pd

# Đọc file CSV vào DataFrame
df = pd.read_csv("chinh_phu_2.csv")

# Chọn các cột cần lọc
selected_columns = [
    "url", "post_id", "content", "date_posted", "num_comments", "num_shares", 
    "num_likes_type", "original_post", "attachments", "video_view_count", "likes", 
    "count_reactions_type", "post_image", "post_type", "page_reviews_score"
]

# Lọc dữ liệu
df_filtered = df[selected_columns]

# Xuất ra file CSV mới nếu cần
df_filtered.to_csv("chinh_phu_2_result.csv", index=False)

# Hiển thị vài dòng đầu để kiểm tra dữ liệu
print(df_filtered.head())