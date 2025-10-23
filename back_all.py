import os
import uuid
import pandas as pd
import requests
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import traceback
import io
import json
import logging

# =============================================================================
# 🎯 可配置参数区域 - 所有重要参数集中在这里
# =============================================================================

# --- Flask 服务配置 ---
FLASK_HOST = '0.0.0.0'                    # Flask服务监听地址
FLASK_PORT = 8520                          # Flask服务端口
FLASK_DEBUG = True                         # 是否启用调试模式

# --- 文件URL配置 ---
FILE_SERVER_HOST = '192.168.131.59'     # 文件服务器地址
FILE_SERVER_PORT = '8520'                # 文件服务器端口

# --- 文件路径配置 ---
UPLOAD_FOLDER = 'temp'                     # 上传文件临时存储目录
DOWNLOAD_FOLDER = os.path.join('static', 'downloads')  # 下载文件存储目录
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}     # 允许的文件扩展名

# --- Dify API 配置 ---
DIFY_API_BASE_URL = 'http://192.168.125.223/v1'      # Dify API基础URL
DIFY_API_KEY = 'Bearer app-6h0jlXree8oBQ10Yyjyk3eCk'  # Dify API认证密钥
DIFY_INPUT_VARIABLE_NAME = 'uploaded_file'             # 工作流输入变量名
DIFY_OUTPUT_VARIABLE_NAME = 'download_link'            # 工作流输出变量名

# --- 工作流处理配置 ---
DEFAULT_CHUNK_SIZE = 30                   # 默认每个chunk的行数（增加chunk大小减少任务数量）
MAX_WORKERS = 6                          # 线程池最大工作线程数（提高并发度）
MAX_RETRIES = -1                          # 单个chunk最大重试次数（无限重试，确保每个chunk都被分析）
RETRY_DELAY = 1                           # 重试间隔时间（秒）
REQUEST_TIMEOUT = 180                     # 请求超时时间（秒）
FILE_DOWNLOAD_TIMEOUT = 60              # 文件下载超时时间（秒）

# --- 列名配置 ---
ID_COLUMN_NAME = 'id'                      # 默认ID列名
POSSIBLE_ID_COLUMNS = ['id', 'ID', '编号', '序号']  # 可能的ID列名列表
COLUMNS_TO_REMOVE = ['id', 'ID']  # 最终需要删除的ID列名

# --- 调试配置 ---
ENABLE_DEBUG_PRINT = True               # 是否启用调试打印
MAX_DEBUG_OUTPUT_LENGTH = 500             # 调试输出的最大长度

# --- 错误处理配置 ---
ENABLE_TRACEBACK_PRINT = True              # 是否打印错误堆栈信息

# --- 文件处理配置 ---
GENERATE_UNIQUE_FILENAMES = True          # 是否生成唯一文件名（防止冲突）
UUID_LENGTH = 8                            # 唯一文件名中UUID的长度

# =============================================================================
# ⚙️ 自动生成的URL配置（通常不需要修改）
# =============================================================================
DIFY_FILE_UPLOAD_URL = f"{DIFY_API_BASE_URL}/files/upload"
DIFY_WORKFLOW_RUN_URL = f"{DIFY_API_BASE_URL}/workflows/run"

# =============================================================================
# 🚀 初始化 Flask 应用
# =============================================================================
app = Flask(__name__)
# 配置CORS以允许前端访问
CORS(app, origins=['http://192.168.131.59:8523', 'http://localhost:8523'], 
     methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
     allow_headers=['Content-Type', 'Authorization'])

# 确保必要的文件夹存在
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
if not os.path.exists(DOWNLOAD_FOLDER):
    os.makedirs(DOWNLOAD_FOLDER)

# =============================================================================
# 📋 配置信息打印
# =============================================================================
if ENABLE_DEBUG_PRINT:
    print(f"=== 🎯 Dify 配置信息 ===")
    print(f"输入变量名: '{DIFY_INPUT_VARIABLE_NAME}'")
    print(f"输出变量名: '{DIFY_OUTPUT_VARIABLE_NAME}'")
    print(f"API端点: {DIFY_API_BASE_URL} (本地部署)")
    print(f"认证方式: {DIFY_API_KEY[:20]}...")  # 显示认证前缀和密钥部分
    print(f"工作线程数: {MAX_WORKERS}")
    print(f"最大重试次数: {'无限重试' if MAX_RETRIES == -1 else MAX_RETRIES}")
    print(f"默认Chunk大小: {DEFAULT_CHUNK_SIZE}")
    print(f"文件服务器: {FILE_SERVER_HOST}:{FILE_SERVER_PORT}")
    print(f"====================")




def process_streaming_response(response):
    """
    处理Dify的streaming响应，获取工作流的最终输出
    """
    if response.status_code != 200:
        # 保存响应文本用于错误信息
        error_text = response.text if hasattr(response, 'text') else "无法获取错误信息"
        raise ValueError(f"请求失败，状态码: {response.status_code}, 错误信息: {error_text}")
    
    full_response = ""
    all_events = []  # 收集所有事件
    
    try:
        # 收集所有响应行
        for line in response.iter_lines():
            if not line:
                continue
                
            if not line.startswith(b'data:'):
                continue
                
            data_content = line.decode('utf-8')[5:].strip()
            if data_content == '[DONE]':
                break
                
            try:
                message = json.loads(data_content)
                all_events.append(message)
                
                # 查找工作流完成事件
                if message.get('event') == 'workflow_finished':
                    # 工作流完成事件包含最终输出
                    if 'data' in message and 'outputs' in message['data']:
                        full_response = json.dumps(message['data'])
                        break
                elif message.get('event') == 'node_finished':
                    # 节点完成事件，检查是否是结束节点
                    node_data = message.get('data', {})
                    if node_data.get('node_type') == 'end' and 'outputs' in node_data:
                        full_response = json.dumps(node_data)
                        break
                elif 'data' in message and 'outputs' in message['data']:
                    # 通用数据输出
                    full_response = json.dumps(message['data'])
                    
            except json.JSONDecodeError:
                logging.error(f"JSON解析错误: {data_content}")
                continue
                
    except Exception as e:
        logging.error(f"处理streaming响应时出错: {e}")
        
    finally:
        response.close()
    
    # 如果没有找到特定的工作流完成事件，返回最后一个包含数据的事件
    if not full_response and all_events:
        for event in reversed(all_events):
            if 'data' in event and 'outputs' in event['data']:
                full_response = json.dumps(event['data'])
                break
    
    return full_response




# 3. 并行任务单元函数
def call_small_workflow(chunk_id, df_chunk, which_aspects_value=None):
    print(f"开始处理 Chunk #{chunk_id}...")
    
    try:
        # --- 第一步: 保存切分文件到可访问位置并生成URL ---
        print(f"正在为 Chunk #{chunk_id} 保存文件并生成访问URL...")
        
        # 生成唯一文件名
        unique_filename = f"chunk_{chunk_id}_{uuid.uuid4().hex[:UUID_LENGTH]}.xlsx"
        file_path = os.path.join(DOWNLOAD_FOLDER, unique_filename)
        
        # 保存Excel文件
        df_chunk.to_excel(file_path, index=False)
        
        # 生成文件访问URL (使用当前服务的端口)
        file_url = f"http://{FILE_SERVER_HOST}:{FILE_SERVER_PORT}/downloads/{unique_filename}"
        print(f"Chunk #{chunk_id} 文件已保存，访问URL: {file_url}")

        # --- 第二步: 运行工作流 (使用文件URL作为输入) ---
        # 使用传入的which_aspects_value，如果没有则使用默认值
        if which_aspects_value is None:
            which_aspects_value = "水质、水务、水利的招标信息数据" # 硬编码恢复
        
        print(f"使用which_aspects值: {which_aspects_value}")

        payload = {
            "inputs": {
                DIFY_INPUT_VARIABLE_NAME: {
                    "type": "document",
                    "transfer_method": "remote_url",
                    "url": file_url
                },
                "which_aspects": which_aspects_value  # 直接传递字符串值，不需要包装成对象
            },
            "response_mode": "streaming",  # 参考 func.py，使用 streaming 模式
            "user": 'backend_service_user'
        }
        headers_run = {'Authorization': DIFY_API_KEY, 'Content-Type': 'application/json'}

        print(f"正在为 Chunk #{chunk_id} 运行工作流...")
        print(f"工作流请求payload: {json.dumps(payload, ensure_ascii=False)}")
        run_response = requests.post(DIFY_WORKFLOW_RUN_URL, headers=headers_run, json=payload, timeout=REQUEST_TIMEOUT, stream=True)
        
        # 打印响应状态码和头信息用于调试
        print(f"工作流响应状态码: {run_response.status_code}")
        print(f"工作流响应头: {dict(run_response.headers)}")
        
        # 特殊处理400错误
        if run_response.status_code == 400:
            error_msg = f"Chunk #{chunk_id} 工作流请求400错误: {run_response.text}"
            print(error_msg)
            return {'chunk_id': chunk_id, 'status': 'FAILED', 'error': error_msg}
        
        run_response.raise_for_status() 
        
        # 处理streaming响应，参考func.py的实现
        streaming_result = process_streaming_response(run_response)
        if ENABLE_DEBUG_PRINT:
            print(f"Chunk #{chunk_id} streaming响应结果: {streaming_result[:MAX_DEBUG_OUTPUT_LENGTH]}...")
        
        # 尝试多种方式解析响应
        result_json = None
        
        # 方式1: 直接解析为JSON
        try:
            result_json = json.loads(streaming_result)
            print(f"Chunk #{chunk_id} 成功解析JSON响应")
        except:
            print(f"Chunk #{chunk_id} 直接JSON解析失败")
        
        # 方式2: 如果直接解析失败，尝试提取JSON部分
        if not result_json:
            try:
                import re
                json_match = re.search(r'\{[\s\S]*\}', streaming_result)
                if json_match:
                    result_json = json.loads(json_match.group(0))
                    print(f"Chunk #{chunk_id} 通过正则提取JSON成功")
            except:
                print(f"Chunk #{chunk_id} 正则提取JSON失败")
        
        # 方式3: 如果还是失败，可能是简单的字符串响应
        if not result_json and streaming_result:
            # 创建一个简单的响应结构
            result_json = {
                "outputs": {
                    DIFY_OUTPUT_VARIABLE_NAME: streaming_result.strip()
                }
            }
            print(f"Chunk #{chunk_id} 使用字符串响应模式")
        
        if not result_json:
            raise ValueError(f"无法从streaming响应中解析有效数据: {streaming_result[:MAX_DEBUG_OUTPUT_LENGTH]}...")
        
        # 打印完整的响应结构用于调试
        if ENABLE_DEBUG_PRINT:
            print(f"Chunk #{chunk_id} 解析后的响应结构: {json.dumps(result_json, ensure_ascii=False, indent=2)}")
        
        # --- 第三步: 获取工作流结果 ---
        # 从streaming响应中提取最终结果
        if ENABLE_DEBUG_PRINT:
            print(f"Chunk #{chunk_id} 工作流运行完成，正在解析结果...")
            print(f"Chunk #{chunk_id} 解析后的响应结构: {json.dumps(result_json, ensure_ascii=False, indent=2)}")
            print(f"Chunk #{chunk_id} 节点类型: {result_json.get('node_type')}")
            print(f"Chunk #{chunk_id} 节点ID: {result_json.get('node_id')}")
            print(f"Chunk #{chunk_id} 是否有outputs: {'outputs' in result_json}")
            if 'outputs' in result_json:
                print(f"Chunk #{chunk_id} outputs键: {list(result_json['outputs'].keys())}")
        
        # 检查是否是结束节点的输出
        if result_json.get('node_type') == 'end' and 'outputs' in result_json:
            # 这是结束节点的输出
            outputs = result_json['outputs']
            if DIFY_OUTPUT_VARIABLE_NAME in outputs:
                download_url = outputs[DIFY_OUTPUT_VARIABLE_NAME]
                print(f"Chunk #{chunk_id} 工作流结束节点返回下载链接: {download_url}")
                
                if download_url and isinstance(download_url, str) and download_url.startswith('http'):
                    if ENABLE_DEBUG_PRINT:
                        print(f"正在为 Chunk #{chunk_id} 下载结果文件...")
                    file_response = requests.get(download_url, timeout=FILE_DOWNLOAD_TIMEOUT)
                    file_response.raise_for_status()
                    
                    df_filtered_chunk = pd.read_excel(io.BytesIO(file_response.content))
                    
                    # 小Dify输出的文件应该包含id和项目名称列
                    if 'id' not in df_filtered_chunk.columns:
                        raise ValueError(f"下载的结果文件中找不到关键列: 'id'")
                        
                    filtered_ids = df_filtered_chunk['id'].tolist()
                    print(f"Chunk #{chunk_id} 从结果文件中解析出 {len(filtered_ids)} 个ID")
                    
                    return {'chunk_id': chunk_id, 'status': 'SUCCESS', 'download_url': download_url, 'data': filtered_ids}
        
        # 检查是否有直接的输出结果（兼容旧格式）
        if 'outputs' in result_json and DIFY_OUTPUT_VARIABLE_NAME in result_json['outputs']:
            # 如果工作流直接返回了输出变量
            result_data = result_json['outputs'][DIFY_OUTPUT_VARIABLE_NAME]
            if ENABLE_DEBUG_PRINT:
                    print(f"Chunk #{chunk_id} 工作流输出变量 '{DIFY_OUTPUT_VARIABLE_NAME}' 的值: {result_data}")
                    print(f"Chunk #{chunk_id} 输出变量类型: {type(result_data)}")
            if isinstance(result_data, str) and result_data.startswith('http'):
                # 如果返回的是下载链接
                download_url = result_data
                print(f"Chunk #{chunk_id} 工作流运行成功, 获得下载链接: {download_url}")
                
                if ENABLE_DEBUG_PRINT:
                    print(f"正在为 Chunk #{chunk_id} 下载结果文件...")
                file_response = requests.get(download_url, timeout=FILE_DOWNLOAD_TIMEOUT)
                file_response.raise_for_status()
                
                df_filtered_chunk = pd.read_excel(io.BytesIO(file_response.content))
                
                if ID_COLUMN_NAME not in df_filtered_chunk.columns:
                    raise ValueError(f"下载的结果文件中找不到关键列: '{ID_COLUMN_NAME}'")
                    
                filtered_ids = df_filtered_chunk[ID_COLUMN_NAME].tolist()
                print(f"Chunk #{chunk_id} 从结果文件中解析出 {len(filtered_ids)} 个ID")
                
                # 返回下载链接而不是ID列表，用于后续合并
                return {'chunk_id': chunk_id, 'status': 'SUCCESS', 'download_url': download_url, 'data': filtered_ids}
            elif isinstance(result_data, list):
                # 如果直接返回了ID列表
                filtered_ids = result_data
                print(f"Chunk #{chunk_id} 从工作流输出中获得 {len(filtered_ids)} 个ID")
                return {'chunk_id': chunk_id, 'status': 'SUCCESS', 'data': filtered_ids}
            else:
                # 如果返回了其他格式，尝试解析
                filtered_ids = []
                print(f"Chunk #{chunk_id} 工作流返回了非预期的数据格式: {type(result_data)}")
                return {'chunk_id': chunk_id, 'status': 'SUCCESS', 'data': filtered_ids}
        else:
            # 如果没有直接的输出，检查是否有下载链接
            download_url = result_json.get('outputs', {}).get(DIFY_OUTPUT_VARIABLE_NAME)
            
            if download_url and isinstance(download_url, str):
                print(f"Chunk #{chunk_id} 工作流运行成功, 获得下载链接: {download_url}")
                
                print(f"正在为 Chunk #{chunk_id} 下载结果文件...")
                file_response = requests.get(download_url, timeout=60)
                file_response.raise_for_status()
                
                df_filtered_chunk = pd.read_excel(io.BytesIO(file_response.content))
                
                if ID_COLUMN_NAME not in df_filtered_chunk.columns:
                    raise ValueError(f"下载的结果文件中找不到关键列: '{ID_COLUMN_NAME}'")
                    
                filtered_ids = df_filtered_chunk[ID_COLUMN_NAME].tolist()
                print(f"Chunk #{chunk_id} 从结果文件中解析出 {len(filtered_ids)} 个ID")
                
                # 返回下载链接而不是ID列表，用于后续合并
                return {'chunk_id': chunk_id, 'status': 'SUCCESS', 'download_url': download_url, 'data': filtered_ids}
            else:
                # 如果既没有直接输出也没有下载链接，返回空列表
                filtered_ids = []
                print(f"Chunk #{chunk_id} 工作流未返回有效的结果，返回空列表")
                return {'chunk_id': chunk_id, 'status': 'SUCCESS', 'data': filtered_ids}

    except Exception as e:
        error_message = f"处理Chunk #{chunk_id}时发生错误: {e}"
        # 避免多次读取响应内容，只记录基本错误信息
        print(error_message)
        if ENABLE_TRACEBACK_PRINT:
            traceback.print_exc()
        return {'chunk_id': chunk_id, 'status': 'FAILED', 'error': error_message}


# 4. 添加文件下载路由
# 代理Dify API的路由
@app.route('/v1/files/upload', methods=['POST'])
def proxy_dify_file_upload():
    """代理Dify文件上传API"""
    try:
        print(f"=== 文件上传代理请求 ===")
        print(f"请求方法: {request.method}")
        print(f"Content-Type: {request.content_type}")
        print(f"文件列表: {list(request.files.keys())}")
        print(f"表单数据: {dict(request.form)}")
        
        if 'file' not in request.files:
            return jsonify({"error": "没有找到文件"}), 400
            
        uploaded_file = request.files['file']
        print(f"上传文件名: {uploaded_file.filename}")
        print(f"文件大小: {len(uploaded_file.read())} bytes")
        uploaded_file.seek(0)  # 重置文件指针
        print(f"文件MIME类型: {uploaded_file.content_type}")
        
        # 检查文件扩展名
        if uploaded_file.filename:
            file_ext = uploaded_file.filename.rsplit('.', 1)[-1].lower()
            print(f"文件扩展名: {file_ext}")
            if file_ext not in ALLOWED_EXTENSIONS:
                return jsonify({
                    "code": "unsupported_file_type",
                    "message": f"不支持的文件类型: {file_ext}. 支持的类型: {', '.join(ALLOWED_EXTENSIONS)}",
                    "status": 415
                }), 415
        
        # 准备转发到Dify API的数据
        files = {
            'file': (uploaded_file.filename, uploaded_file.stream, uploaded_file.content_type)
        }
        
        data = {}
        if 'user' in request.form:
            data['user'] = request.form['user']
            
        headers = {
            'Authorization': DIFY_API_KEY
        }
        
        print(f"转发到Dify API: {DIFY_FILE_UPLOAD_URL}")
        print(f"认证头: {DIFY_API_KEY[:20]}...")
        
        response = requests.post(
            DIFY_FILE_UPLOAD_URL,
            files=files,
            data=data,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        
        print(f"Dify API响应状态: {response.status_code}")
        print(f"Dify API响应内容: {response.text}")
        
        if response.status_code == 200:
            return jsonify(response.json()), response.status_code
        else:
            # 如果Dify返回错误，返回相同的错误
            try:
                error_data = response.json()
                return jsonify(error_data), response.status_code
            except:
                return jsonify({
                    "code": "dify_api_error",
                    "message": f"Dify API错误: {response.text}",
                    "status": response.status_code
                }), response.status_code
        
    except Exception as e:
        print(f"代理文件上传错误: {e}")
        if ENABLE_TRACEBACK_PRINT:
            traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/v1/workflows/run', methods=['POST'])
def proxy_dify_workflow():
    """代理Dify工作流API"""
    try:
        request_data = request.get_json()
        print(f"=== 工作流API调用 ===")
        print(f"请求数据: {json.dumps(request_data, indent=2, ensure_ascii=False)}")
        
        # 转发请求到真实的Dify API
        headers = {
            'Authorization': DIFY_API_KEY,
            'Content-Type': 'application/json'
        }
        
        print(f"转发到Dify API: {DIFY_WORKFLOW_RUN_URL}")
        print(f"认证头: {DIFY_API_KEY[:20]}...")
        
        response = requests.post(
            DIFY_WORKFLOW_RUN_URL,
            json=request_data,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        
        print(f"Dify API响应状态: {response.status_code}")
        print(f"Dify API响应内容: {response.text}")
        
        # 检查响应是否为JSON格式
        try:
            response_json = response.json()
            return jsonify(response_json), response.status_code
        except ValueError as json_error:
            # 如果不是JSON格式，返回原始文本
            print(f"响应不是JSON格式: {json_error}")
            return response.text, response.status_code, {'Content-Type': response.headers.get('Content-Type', 'text/plain')}
        
    except requests.exceptions.RequestException as req_error:
        print(f"请求错误: {req_error}")
        return jsonify({"error": f"请求Dify API失败: {str(req_error)}"}), 500
    except Exception as e:
        print(f"代理工作流错误: {e}")
        if ENABLE_TRACEBACK_PRINT:
            traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/downloads/<filename>')
def download_file(filename):
    """
    从DOWNLOAD_FOLDER目录中提供静态文件下载。
    """
    return send_from_directory(DOWNLOAD_FOLDER, filename, as_attachment=True)

# 5. 主API端点 (无变化)
@app.route('/process-large-excel', methods=['POST'])
def process_large_excel():
    print(f"Request Method: {request.method}")
    print(f"Request URL: {request.url}")
    print(f"Request Headers: {request.headers}")
    print(f"Request Content-Type: {request.content_type}")
    
    # 尝试获取原始请求数据，无论Content-Type是什么
    try:
        raw_data = request.get_data()
        print(f"Raw Request Data (first 500 bytes): {raw_data[:500].decode('utf-8', errors='ignore')}")
    except Exception as e:
        print(f"Error getting raw request data: {e}")

    # 尝试解析JSON数据，如果Content-Type是application/json
    if request.is_json:
        try:
            json_data = request.get_json(silent=True)
            print(f"Parsed JSON Data: {json.dumps(json_data, ensure_ascii=False, indent=2)}")
        except Exception as e:
            print(f"Error parsing JSON data: {e}")
    else:
        print("Request is not JSON.")
    if 'file' not in request.files: return jsonify({"error": "请求中没有找到文件部分"}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({"error": "没有选择文件"}), 400
    
    # 获取which_aspects参数
    which_aspects = request.form.get('which_aspects', '水质、水务、水利的招标信息数据')  # 默认值
    print(f"接收到的which_aspects参数: {which_aspects}")
    
    if file:
        large_excel_path = os.path.join(UPLOAD_FOLDER, f"large_{uuid.uuid4().hex[:UUID_LENGTH]}.xlsx")
        file.save(large_excel_path)
        start_time = time.time()

        try:
            df_large = pd.read_excel(large_excel_path)
            # 真正新增一列自增 id，而不是用旧索引
            df_large.insert(0, ID_COLUMN_NAME, range(len(df_large)))
            print(f"成功加载Excel并自动添加了 '{ID_COLUMN_NAME}' 列。总行数: {len(df_large)}")
            chunk_size = DEFAULT_CHUNK_SIZE  # 从配置读取
            print(f"使用chunk大小: {chunk_size} 行")
            # 创建chunk列表 - 切分后自行并发调用，无需按顺序
            df_chunks = [(i // chunk_size, df_large.iloc[i:i + chunk_size]) for i in range(0, len(df_large), chunk_size)]
            total_chunks = len(df_chunks)
            
            # 存储所有结果的线程安全容器
            import threading
            results_lock = threading.Lock()
            all_filtered_ids = []
            download_urls = []
            final_results_df = pd.DataFrame()
            final_results_json = []
            retry_count = 0
            
            # 跟踪chunk处理状态
            chunk_status = {}
            for chunk_id, _ in df_chunks:
                chunk_status[chunk_id] = {'status': 'pending', 'retries': 0}

            def process_chunk_concurrent(chunk_id, chunk_df, which_aspects_value):
                """并发处理单个chunk，支持无限重试直到成功"""
                retry = 0
                while True:  # 无限循环直到成功
                    try:
                        result = call_small_workflow(chunk_id, chunk_df, which_aspects_value)
                        if result['status'] == 'SUCCESS':
                            # 线程安全地处理结果
                            with results_lock:
                                chunk_status[chunk_id]['status'] = 'success'  # 标记为成功状态
                                if result.get('download_url'):
                                    download_urls.append(result['download_url'])
                                    # 实时处理结果数据
                                    try:
                                        file_response = requests.get(result['download_url'], timeout=FILE_DOWNLOAD_TIMEOUT)
                                        file_response.raise_for_status()
                                        df_chunk_result = pd.read_excel(io.BytesIO(file_response.content))
                                        
                                        if 'id' in df_chunk_result.columns:
                                            chunk_ids = df_chunk_result['id'].tolist()
                                            
                                            # 过滤掉已经处理过的ID，避免重复
                                            new_chunk_ids = [cid for cid in chunk_ids if cid not in all_filtered_ids]
                                            all_filtered_ids.extend(new_chunk_ids)
                                            
                                            if new_chunk_ids:  # 只处理新的ID
                                                # 使用小 Dify 返回的 id 从 df_large 中提取完整行
                                                matched_rows = df_large[df_large[ID_COLUMN_NAME].isin(new_chunk_ids)].copy()
                                                
                                                # 移除 ID 列
                                                matched_rows = matched_rows.drop(columns=[ID_COLUMN_NAME], errors='ignore')
                                                
                                                # 假设关键词列名为 '关键词'，将其移到第一列（如果存在）
                                                if '关键词' in matched_rows.columns:
                                                    columns = ['关键词'] + [col for col in matched_rows.columns if col != '关键词']
                                                    matched_rows = matched_rows[columns]
                                                
                                                # 合并到最终 DataFrame
                                                final_results_df = pd.concat([final_results_df, matched_rows], ignore_index=True)
                                                
                                                # 将每行数据转换为字典并添加到结果列表
                                                for _, row in matched_rows.iterrows():
                                                    row_dict = row.to_dict()
                                                    # 删除可能的剩余 ID 列
                                                    for col in COLUMNS_TO_REMOVE:
                                                        if col in row_dict:
                                                            del row_dict[col]
                                                    final_results_json.append(row_dict)
                                    except Exception:
                                        pass
                            return {'status': 'SUCCESS', 'chunk_id': chunk_id, 'download_url': result.get('download_url', '')}
                        else:
                            # 失败重试
                            retry += 1
                            with results_lock:
                                chunk_status[chunk_id]['retries'] += 1
                            print(f"Chunk #{chunk_id} 第{retry}次重试...")
                            time.sleep(RETRY_DELAY)
                    except Exception as e:
                        # 异常重试
                        retry += 1
                        with results_lock:
                            chunk_status[chunk_id]['retries'] += 1
                        print(f"Chunk #{chunk_id} 第{retry}次重试，异常: {str(e)[:100]}...")
                        time.sleep(RETRY_DELAY)

            # 并发处理所有chunk - 完全随机并发，不限制顺序
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # 一次性提交所有任务，让线程池自由调度
                future_to_chunk = {
                    executor.submit(process_chunk_concurrent, chunk_id, chunk_df, which_aspects): chunk_id 
                    for chunk_id, chunk_df in df_chunks
                }
                
                # 实时处理完成的任务（无需等待批次）
                for future in as_completed(future_to_chunk):
                    result = future.result()
                    if result and result.get('status') == 'FAILED':
                        chunk_id = result.get('chunk_id', '未知')
                        print(f"Chunk {chunk_id} 处理失败: {result.get('error', '未知错误')}")
                    elif result and result.get('status') == 'SUCCESS':
                        chunk_id = result.get('chunk_id', '未知')
                        print(f"Chunk {chunk_id} 处理成功")
                        
            # 显示最终处理统计
            successful_chunks = len([cid for cid, status in chunk_status.items() if status['status'] == 'success'])
            total_retries = sum([status['retries'] for status in chunk_status.values()])
            print(f"处理完成 - 总chunk数: {total_chunks}, 成功: {successful_chunks}")
            
            # 如果没有实时汇总数据，使用备用处理方式
            if len(final_results_json) == 0:
                print("使用备用处理方式：下载所有chunk结果文件并重新处理")
                
                # 用于去重的ID集合
                processed_ids = set()
                
                # 下载所有成功的chunk结果文件
                for download_url in download_urls:
                    try:
                        # 下载文件
                        file_response = requests.get(download_url, timeout=FILE_DOWNLOAD_TIMEOUT)
                        file_response.raise_for_status()
                        
                        # 读取Excel数据
                        df_chunk_result = pd.read_excel(io.BytesIO(file_response.content))
                        
                        if not df_chunk_result.empty and 'id' in df_chunk_result.columns:
                            # 获取小Dify返回的ID列表
                            chunk_ids = df_chunk_result['id'].tolist()
                            
                            # 过滤掉已经处理过的ID，避免重复
                            new_chunk_ids = [cid for cid in chunk_ids if cid not in processed_ids]
                            processed_ids.update(new_chunk_ids)
                            
                            if new_chunk_ids:  # 只处理新的ID
                                # 使用ID从原始数据中提取完整行（保持原始数据完整性）
                                matched_rows = df_large[df_large[ID_COLUMN_NAME].isin(new_chunk_ids)].copy()
                                
                                # 移除ID列
                                matched_rows = matched_rows.drop(columns=[ID_COLUMN_NAME], errors='ignore')
                                
                                # 将关键词列移到第一列（如果存在）
                                if '关键词' in matched_rows.columns:
                                    columns = ['关键词'] + [col for col in matched_rows.columns if col != '关键词']
                                    matched_rows = matched_rows[columns]
                                
                                # 合并到最终DataFrame
                                final_results_df = pd.concat([final_results_df, matched_rows], ignore_index=True)
                                
                                # 转换为JSON格式
                                for _, row in matched_rows.iterrows():
                                    row_dict = row.to_dict()
                                    # 删除可能的剩余ID列
                                    for col in COLUMNS_TO_REMOVE:
                                        if col in row_dict:
                                            del row_dict[col]
                                    final_results_json.append(row_dict)
                                
                    except Exception as e:
                        print(f"处理下载文件失败: {str(e)}")
                        continue
                    

            
            # 在保存最终结果文件之前进行排序和去重
            if not final_results_df.empty:
                # 先去重 - 基于所有列的组合去重
                print(f"去重前记录数: {len(final_results_df)}")
                final_results_df = final_results_df.drop_duplicates()
                print(f"去重后记录数: {len(final_results_df)}")
                
                # 确保关键词和时间列存在
                if '关键词' in final_results_df.columns and '时间' in final_results_df.columns:
                    print("正在对最终结果进行排序：先按关键词，再按时间...")
                    # 先按关键词排序，同类别内再按时间排序
                    final_results_df = final_results_df.sort_values(['关键词', '时间'], ascending=[True, True])
                    print(f"排序完成，共 {len(final_results_df)} 条记录")
                else:
                    print("警告：未找到关键词或时间列，跳过排序")
                    
                # 重新构建final_results_json以确保与DataFrame一致
                final_results_json = []
                for _, row in final_results_df.iterrows():
                    row_dict = row.to_dict()
                    # 删除可能的剩余ID列
                    for col in COLUMNS_TO_REMOVE:
                        if col in row_dict:
                            del row_dict[col]
                    final_results_json.append(row_dict)
            
            # 保存最终结果文件
            final_filename = f"final_result_{uuid.uuid4().hex[:UUID_LENGTH]}.xlsx"
            final_filepath = os.path.join(DOWNLOAD_FOLDER, final_filename)
            
            # 使用最终处理的数据
            final_df_to_save = final_results_df
            
            # 保存文件 - 确保移除所有可能的ID列和索引列
            # 检查并删除所有可能的ID列
            for col in COLUMNS_TO_REMOVE:
                if col in final_df_to_save.columns:
                    final_df_to_save = final_df_to_save.drop(columns=[col])
            
            # 检查是否有数字索引列（通常是第一列）
            if len(final_df_to_save.columns) > 0:
                first_col = final_df_to_save.columns[0]
                # 如果第一列是数字且不是预期的关键词列，则删除它
                if first_col.isdigit() or first_col in ['index', 'Unnamed: 0']:
                    final_df_to_save = final_df_to_save.drop(columns=[first_col])
            
            # 确保不保存索引作为列
            final_df_to_save.to_excel(final_filepath, index=False)
            
            # 上传文件到文件服务器
            try:
                with open(final_filepath, 'rb') as f:
                    files = {'file': (final_filename, f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
                    upload_response = requests.post(f"http://{FILE_SERVER_HOST}:{FILE_SERVER_PORT}/upload", files=files)
                    upload_response.raise_for_status()
                    final_download_url = upload_response.json().get('download_url', '')
            except Exception as e:
                final_download_url = f"http://{FILE_SERVER_HOST}:{FILE_SERVER_PORT}/downloads/{final_filename}"
            
            end_time = time.time()
            
            # 构建返回结果
            successful_chunks = len([cid for cid, status in chunk_status.items() if status['status'] == 'success'])
            response_data = {
                "message": "处理完成",
                "summary": { 
                    "total_chunks": total_chunks, 
                    "successful_chunks": successful_chunks,
                    "chunk_size": chunk_size,
                    "retry_mode": "infinite_retries"  # 标识使用无限重试模式
                },
                "processing_time": f"{end_time - start_time:.2f} 秒",
                "total_filtered_count": len(final_results_json),
                "filtered_data": final_results_json
            }
            
            # 如果有最终下载链接，添加到响应中
            if final_download_url:
                response_data["final_download_url"] = final_download_url
            
            return jsonify(response_data)

        except Exception as e:
            if ENABLE_TRACEBACK_PRINT:
                traceback.print_exc()
            return jsonify({"error": "服务器内部错误", "details": str(e), "trace": traceback.format_exc() if ENABLE_TRACEBACK_PRINT else None}), 500
        finally:
            if os.path.exists(large_excel_path): os.remove(large_excel_path)


    return jsonify({"error": "文件处理失败"}), 500


# 5. 启动Web服务
if __name__ == '__main__':
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=FLASK_DEBUG)