# app.py

import os
import uuid # 用于生成唯一的文件名，防止文件被覆盖
from flask import Flask, request, jsonify, send_from_directory
import pandas as pd

# 1. 初始化 Flask 应用
app = Flask(__name__)

# 2. 配置一个用于存放生成的Excel文件的文件夹
#    我们将其放在一个名为 'static/downloads' 的子目录中
DOWNLOAD_FOLDER = os.path.join('static', 'downloads')
if not os.path.exists(DOWNLOAD_FOLDER):
    os.makedirs(DOWNLOAD_FOLDER)

# 3. 创建核心API端点，Dify将向这里发送POST请求
@app.route('/generate-excel', methods=['POST'])
def generate_excel():
    """
    接收JSON数据，生成Excel文件，并返回下载链接。
    """
    try:
        # a. 从POST请求中获取JSON数据
        payload = request.get_json()
        if not payload:
            return jsonify({"error": "请求中没有找到JSON数据"}), 400

        # b. 从JSON中提取我们关心的数据列表 (键名为'data')
        data_list = payload.get('data')
        if not isinstance(data_list, list) or not data_list:
            return jsonify({"error": "JSON数据格式不正确或列表为空"}), 400

        # c. 使用pandas将数据列表转换为DataFrame
        df = pd.DataFrame(data_list)

        # d. 生成一个唯一的、安全的文件名，避免冲突
        #    例如：'output_a1b2c3d4.xlsx'
        unique_filename = f"output_{uuid.uuid4().hex[:8]}.xlsx"
        file_path = os.path.join(DOWNLOAD_FOLDER, unique_filename)

        # e. 将DataFrame保存为Excel文件
        #    index=False 表示在Excel中不保存DataFrame的行索引
        df.to_excel(file_path, index=False)

        # f. 构建可供公网访问的下载URL
        #    这需要您的服务器有一个公网IP或域名
        #    例如: http://123.45.67.89:5001/downloads/output_a1b2c3d4.xlsx
        server_url = request.host_url
        download_url = f"{server_url}downloads/{unique_filename}"
        
        # g. 返回包含下载链接的JSON响应
        return jsonify({"download_url": download_url})

    except Exception as e:
        # 如果过程中出现任何错误，返回一个错误信息
        print(f"发生错误: {e}")
        return jsonify({"error": "服务器内部错误"}), 500

# 4. 创建一个用于提供文件下载的路由
#    当用户点击Dify返回的链接时，会访问这个端点来获取文件
@app.route('/downloads/<filename>')
def download_file(filename):
    """
    从DOWNLOAD_FOLDER目录中提供静态文件下载。
    """
    return send_from_directory(DOWNLOAD_FOLDER, filename, as_attachment=True)


# 5. 启动Web服务
if __name__ == '__main__':
    # host='0.0.0.0' 表示监听所有网络接口，这样公网才能访问
    # port=5001 是服务运行的端口号，可以根据需要修改
    app.run(host='0.0.0.0', port=8521, debug=True)