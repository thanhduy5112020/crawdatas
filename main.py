import requests
import json
import pandas as pd
import time

def get_fb_comments(post_id, access_token, limit=100):
    """
    Lấy bình luận từ một bài đăng Facebook cụ thể
    
    Parameters:
    - post_id: ID của bài đăng Facebook
    - access_token: Token truy cập Facebook Graph API
    - limit: Số lượng bình luận tối đa muốn lấy
    
    Returns:
    - DataFrame chứa dữ liệu bình luận
    """
    # URL cơ sở cho Graph API
    base_url = f"https://graph.facebook.com/v16.0/{post_id}/comments"
    
    # Tham số truy vấn
    params = {
        "access_token": access_token,
        "limit": 25,  # Số bình luận mỗi request
        "fields": "id,message,created_time,like_count,comment_count,from"
    }
    
    all_comments = []
    next_url = base_url
    
    # Lặp qua từng trang kết quả
    while next_url and len(all_comments) < limit:
        try:
            # Gửi request đến API
            response = requests.get(next_url, params=params)
            response.raise_for_status()
            
            # Phân tích dữ liệu JSON
            data = response.json()
            
            # Thêm bình luận vào danh sách
            if 'data' in data:
                all_comments.extend(data['data'])
            
            # Kiểm tra xem còn trang kế tiếp không
            if 'paging' in data and 'next' in data['paging']:
                next_url = data['paging']['next']
                params = {}  # Xóa params vì URL paging đã có tất cả tham số
            else:
                next_url = None
                
            # Tạm dừng để tránh bị giới hạn tốc độ
            time.sleep(1)
            
        except Exception as e:
            print(f"Lỗi khi crawl: {e}")
            break
    
    # Chuyển đổi sang DataFrame
    df = pd.DataFrame(all_comments)
    
    # Xử lý dữ liệu
    if not df.empty and 'from' in df.columns:
        # Tách thông tin người dùng
        df['user_id'] = df['from'].apply(lambda x: x.get('id') if x else None)
        df['user_name'] = df['from'].apply(lambda x: x.get('name') if x else None)
        df.drop('from', axis=1, inplace=True)
    
    return df

def crawl_multiple_posts(post_ids, access_token):
    """
    Lấy bình luận từ nhiều bài đăng Facebook
    
    Parameters:
    - post_ids: Danh sách ID bài đăng Facebook
    - access_token: Token truy cập Facebook Graph API
    
    Returns:
    - DataFrame chứa dữ liệu bình luận từ tất cả bài đăng
    """
    all_post_comments = []
    
    for post_id in post_ids:
        print(f"Đang crawl bình luận cho bài đăng {post_id}...")
        post_comments = get_fb_comments(post_id, access_token)
        
        if not post_comments.empty:
            # Thêm post_id vào DataFrame
            post_comments['post_id'] = post_id
            all_post_comments.append(post_comments)
    
    # Kết hợp tất cả DataFrame
    if all_post_comments:
        final_df = pd.concat(all_post_comments, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame()

def save_to_csv(df, filename="facebook_comments.csv"):
    """Lưu DataFrame vào file CSV"""
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"Đã lưu dữ liệu vào {filename}")

# Ví dụ sử dụng
if __name__ == "__main__":
    # Thay thế bằng token và ID bài đăng thực tế của bạn
    ACCESS_TOKEN = "FACEBOOK_ACCESS_TOKEN"
    POST_IDS = [
        "PageID_PostID1",
        "PageID_PostID2"
    ]
    
    # Crawl dữ liệu
    comments_df = crawl_multiple_posts(POST_IDS, ACCESS_TOKEN)
    
    # Lưu dữ liệu
    if not comments_df.empty:
        save_to_csv(comments_df)
        print(f"Đã crawl được {len(comments_df)} bình luận.")
    else:
        print("Không tìm thấy bình luận nào.")