import os
import uuid
import pandas as pd
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import traceback
import io

# 1. 初始化 Flask 应用
app = Flask(__name__)
CORS(app)

# 2. 配置文件夹
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER): os.makedirs(UPLOAD_FOLDER)


# --- 配置区 ---
DIFY_INPUT_VARIABLE_NAME = 'uploaded_file'
DIFY_OUTPUT_VARIABLE_NAME = 'final_url_string' 
ID_COLUMN_NAME = 'temp_id' 
DIFY_API_BASE_URL = 'https://api.dify.ai/v1' 
DIFY_API_KEY = 'app-eCBxSW1KSzCl0OlZvYeAJise'
DIFY_FILE_UPLOAD_URL = f"{DIFY_API_BASE_URL}/files/upload"
DIFY_WORKFLOW_RUN_URL = f"{DIFY_API_BASE_URL}/workflows/run"
# --- 配置区结束 ---


# 3. 并行任务单元函数
def call_small_workflow(chunk_id, df_chunk):
    print(f"开始处理 Chunk #{chunk_id}...")
    
    try:
        # --- 第一步: 上传文件 (*** 已按照您的指正进行修正 ***) ---
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_chunk.to_excel(writer, index=False, sheet_name='Sheet1')
        excel_data = output.getvalue()

        # 1. 准备文件部分 (files)
        files = {
            'file': (f'chunk_{chunk_id}.xlsx', excel_data, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        }
        # 2. 准备其他表单数据 (data)
        data = {
            'user': f"user_chunk_{chunk_id}_{uuid.uuid4().hex}"
        }
        # 3. 准备请求头 (headers)
        headers_upload = {'Authorization': f'Bearer {DIFY_API_KEY}'}
        
        print(f"正在为 Chunk #{chunk_id} 上传文件 (采用标准方式)...")
        # 4. 发送POST请求，同时传递 files 和 data
        upload_response = requests.post(
            DIFY_FILE_UPLOAD_URL, 
            headers=headers_upload, 
            files=files, 
            data=data, 
            timeout=60
        )
        
        upload_response.raise_for_status()
        uploaded_file_info = upload_response.json()
        uploaded_file_id = uploaded_file_info['id']
        print(f"Chunk #{chunk_id} 文件上传成功, 文件ID: {uploaded_file_id}")

        file_reference_object = {
            "type": "file",
            "transfer_method": "local_file",
            "upload_file_id": uploaded_file_id
        }

        # --- 第二步: 运行工作流 (逻辑不变) ---
        payload = {
            "inputs": {
                DIFY_INPUT_VARIABLE_NAME: file_reference_object
            },
            "response_mode": "blocking",
            "user": data['user'] # 复用同一个user标识
        }
        headers_run = {'Authorization': f'Bearer {DIFY_API_KEY}', 'Content-Type': 'application/json'}

        print(f"正在为 Chunk #{chunk_id} 运行工作流...")
        run_response = requests.post(DIFY_WORKFLOW_RUN_URL, headers=headers_run, json=payload, timeout=180)
        run_response.raise_for_status() 
        result_json = run_response.json()
        
        # --- 第三步: 获取下载链接并解析文件 (逻辑不变) ---
        download_url = result_json.get('outputs', {}).get(DIFY_OUTPUT_VARIABLE_NAME)
        
        if not download_url or not isinstance(download_url, str):
            raise ValueError(f"Dify未返回有效的下载链接(URL): {download_url}")
            
        print(f"Chunk #{chunk_id} 工作流运行成功, 获得下载链接: {download_url}")
        
        print(f"正在为 Chunk #{chunk_id} 下载结果文件...")
        file_response = requests.get(download_url, timeout=60)
        file_response.raise_for_status()
        
        df_filtered_chunk = pd.read_excel(io.BytesIO(file_response.content))
        
        if ID_COLUMN_NAME not in df_filtered_chunk.columns:
            raise ValueError(f"下载的结果文件中找不到关键列: '{ID_COLUMN_NAME}'")
            
        filtered_ids = df_filtered_chunk[ID_COLUMN_NAME].tolist()
        
        print(f"成功处理 Chunk #{chunk_id}, 从结果文件中解析出 {len(filtered_ids)} 个ID。")
        return {'chunk_id': chunk_id, 'status': 'SUCCESS', 'data': filtered_ids}

    except Exception as e:
        error_message = f"处理Chunk #{chunk_id}时发生错误: {e}"
        if 'run_response' in locals() and run_response.text:
            error_message += f" | Dify Run响应: {run_response.text}"
        elif 'upload_response' in locals() and upload_response.text:
            error_message += f" | Dify Upload响应: {upload_response.text}"
        print(error_message)
        traceback.print_exc()
        return {'chunk_id': chunk_id, 'status': 'FAILED', 'error': error_message}


# 4. 主API端点 (无变化)
@app.route('/process-large-excel', methods=['POST'])
def process_large_excel():
    if 'file' not in request.files: return jsonify({"error": "请求中没有找到文件部分"}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({"error": "没有选择文件"}), 400

    if file:
        large_excel_path = os.path.join(UPLOAD_FOLDER, f"large_{uuid.uuid4().hex[:8]}.xlsx")
        file.save(large_excel_path)
        start_time = time.time()

        try:
            df_large = pd.read_excel(large_excel_path)
            df_large = df_large.reset_index().rename(columns={'index': ID_COLUMN_NAME})
            print(f"成功加载Excel并自动添加了 '{ID_COLUMN_NAME}' 列。总行数: {len(df_large)}")
            chunk_size = 30
            df_chunks = [(i // chunk_size, df_large.iloc[i:i + chunk_size]) for i in range(0, len(df_large), chunk_size)]
            
            total_chunks = len(df_chunks)
            successful_chunks_info, failed_chunks_info, all_filtered_ids = [], [], []

            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_chunk_id = {executor.submit(call_small_workflow, chunk_id, chunk_df): chunk_id for chunk_id, chunk_df in df_chunks}
                for future in as_completed(future_to_chunk_id):
                    result = future.result()
                    if result['status'] == 'SUCCESS':
                        successful_chunks_info.append(result['chunk_id'])
                        if result.get('data'): all_filtered_ids.extend(result['data'])
                    else:
                        failed_chunks_info.append({"chunk_id": result['chunk_id'], "reason": result['error']})
            
            unique_filtered_ids = sorted(list(set(all_filtered_ids)))
            final_results_df = df_large[df_large[ID_COLUMN_NAME].isin(unique_filtered_ids)]
            if ID_COLUMN_NAME in final_results_df.columns:
                 final_results_df = final_results_df.drop(columns=[ID_COLUMN_NAME])
            final_results_json = final_results_df.to_dict(orient='records')
            
            end_time = time.time()
            
            return jsonify({
                "message": "处理完成",
                "summary": { "total_chunks": total_chunks, "successful_chunks": len(successful_chunks_info), "failed_chunks": len(failed_chunks_info), "failed_chunk_details": sorted(failed_chunks_info, key=lambda x: x['chunk_id']) },
                "processing_time": f"{end_time - start_time:.2f} 秒",
                "total_filtered_count": len(final_results_json),
                "filtered_data": final_results_json
            })

        except Exception as e:
            print(f"处理大Excel文件时发生严重错误: {e}\n{traceback.format_exc()}")
            return jsonify({"error": "服务器内部错误", "details": str(e), "trace": traceback.format_exc()}), 500
        finally:
            if os.path.exists(large_excel_path): os.remove(large_excel_path)
            print("已清理上传的主文件。")

    return jsonify({"error": "文件处理失败"}), 500


# 5. 启动Web服务 (无变化)
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8520, debug=True)