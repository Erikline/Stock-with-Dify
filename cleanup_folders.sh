#!/bin/bash

# 清空static和temp文件夹的脚本
# 作者: 系统管理员
# 创建时间: $(date)

# 设置脚本目录
SCRIPT_DIR="/root/QLM-Tender/FrontBack"
STATIC_DIR="$SCRIPT_DIR/static"
TEMP_DIR="$SCRIPT_DIR/temp"

# 日志文件
LOG_FILE="$SCRIPT_DIR/cleanup.log"

# 记录开始时间
echo "$(date '+%Y-%m-%d %H:%M:%S') - 开始清空文件夹" >> "$LOG_FILE"

# 清空static文件夹
if [ -d "$STATIC_DIR" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 清空static文件夹: $STATIC_DIR" >> "$LOG_FILE"
    find "$STATIC_DIR" -type f -delete 2>/dev/null
    find "$STATIC_DIR" -type d -empty -delete 2>/dev/null
    echo "$(date '+%Y-%m-%d %H:%M:%S') - static文件夹清空完成" >> "$LOG_FILE"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - static文件夹不存在: $STATIC_DIR" >> "$LOG_FILE"
fi

# 清空temp文件夹
if [ -d "$TEMP_DIR" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 清空temp文件夹: $TEMP_DIR" >> "$LOG_FILE"
    find "$TEMP_DIR" -type f -delete 2>/dev/null
    find "$TEMP_DIR" -type d -empty -delete 2>/dev/null
    echo "$(date '+%Y-%m-%d %H:%M:%S') - temp文件夹清空完成" >> "$LOG_FILE"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - temp文件夹不存在: $TEMP_DIR" >> "$LOG_FILE"
fi

# 记录结束时间
echo "$(date '+%Y-%m-%d %H:%M:%S') - 文件夹清空任务完成" >> "$LOG_FILE"
echo "----------------------------------------" >> "$LOG_FILE"

# 输出到控制台（可选）
echo "文件夹清空任务完成 - $(date '+%Y-%m-%d %H:%M:%S')"