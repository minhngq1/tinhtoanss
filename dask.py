import yfinance as yf
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt

# Tải dữ liệu chứng khoán cho nhiều mã chứng khoán
tickers = ["AAPL", "MSFT", "TSLA","META"]
df = yf.download(tickers, start="2025-01-01", end="2025-4-21", group_by='ticker')

# Làm phẳng các cột để dễ dàng truy cập
df.columns = ['_'.join(col).strip() for col in df.columns.values]

# Chuyển đổi thành Dask DataFrame
ddf = dd.from_pandas(df, npartitions=3)

# Kiểm tra tên các cột để xác minh xem cột 'Close' có tồn tại không
print("Các cột trong Dask DataFrame:", ddf.columns)

# Thay đổi cách tiếp cận để nhóm theo mã chứng khoán và tính giá trị trung bình cho mỗi cột 'Close'
close_columns = [col for col in ddf.columns if 'Close' in col]
print("Các cột liên quan đến Close:", close_columns)

# Tính giá trị trung bình cho mỗi cột 'Close'
mean_close = ddf[close_columns].mean().compute()
print("Giá trị trung bình của Close cho mỗi mã chứng khoán:\n", mean_close)

# Vẽ biểu đồ cho giá đóng cửa của mỗi mã chứng khoán
df_close = df[close_columns]

# Vẽ biểu đồ
df_close.plot(figsize=(10, 6))
plt.title('Giá đóng cửa của các mã chứng khoán (2025)')
plt.xlabel('Ngày')
plt.ylabel('Giá đóng cửa (USD)')
plt.legend(tickers)
plt.grid(True)
plt.show()
