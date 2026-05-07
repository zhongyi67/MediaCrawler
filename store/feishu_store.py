# -*- coding: utf-8 -*-
"""
飞书多维表格 Store 实现

将爬虫数据写入飞书多维表格，实现 AbstractStore 接口。
支持批量写入、频率控制、错误重试。
"""

import asyncio
import os
import time
from typing import Dict, Optional, List

import aiohttp

from base.base_crawler import AbstractStore
from tools import utils
from var import source_keyword_var

# 飞书基础配置 - 从环境变量读取
APP_ID = os.getenv("FEISHU_APP_ID", "cli_a957745288229cb3")
APP_SECRET = os.getenv("FEISHU_APP_SECRET", "sWXQmWm1vv3HoiZk4kRURcEeqTBPBqes")
APP_TOKEN = os.getenv("FEISHU_APP_TOKEN", "JXvwbEeuJakLvqs0f1IconpJnKx")

# 平台到飞书表ID的映射
PLATFORM_TABLE_MAP = {
    "xhs": "tblWwZpjhpUowxQ6",
    "dy": "tbl2ofGnYLyrKNtn",
    "tieba": "tbl5YzjVKkSIrg4Z",
    "bili": "tblTrWXPa3ZNexvN",
}

# 意向筛选关键词
INTENT_KEYWORDS = [
    "借钱", "贷款", "急用", "周转", "哪里能借",
    "逾期", "利息多少", "无视征信", "口子",
    "怎么办理", "需要多少钱",
]

# 飞书API基础地址
FEISHU_BASE_URL = "https://open.feishu.cn/open-apis"

# 频率控制：每秒最多5次
MAX_REQUESTS_PER_SECOND = 5

# 批量写入配置
BATCH_SIZE = 10
BATCH_TIMEOUT = 2  # 秒


def check_intent(content: str) -> bool:
    """
    检查内容是否包含意向关键词
    """
    if not content:
        return False
    content_lower = content.lower()
    for keyword in INTENT_KEYWORDS:
        if keyword in content:
            return True
    return False


class FeishuTokenManager:
    """飞书 token 管理器，自动获取和缓存 tenant_access_token"""

    def __init__(self):
        self._token: Optional[str] = None
        self._expire_time: int = 0

    async def get_token(self) -> str:
        """获取有效的 tenant_access_token"""
        now = int(time.time())
        if self._token and now < self._expire_time - 60:
            return self._token

        await self._refresh_token()
        return self._token

    async def _refresh_token(self):
        """刷新 tenant_access_token"""
        url = f"{FEISHU_BASE_URL}/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": APP_ID,
            "app_secret": APP_SECRET,
        }
        utils.logger.info("[FeishuTokenManager] 刷新 tenant_access_token...")
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                data = await resp.json()
                if data.get("code") != 0:
                    raise Exception(f"获取飞书 token 失败: {data}")
                self._token = data.get("tenant_access_token")
                self._expire_time = int(time.time()) + data.get("expire", 7200)
                utils.logger.info("[FeishuTokenManager] token 刷新成功")


class FeishuRateLimiter:
    """飞书 API 频率限制器"""

    def __init__(self, max_per_second: int = MAX_REQUESTS_PER_SECOND):
        self.max_per_second = max_per_second
        self._request_times: List[float] = []

    async def acquire(self):
        """获取许可，如果超过频率限制则等待"""
        now = time.time()
        # 移除超过1秒的记录
        self._request_times = [t for t in self._request_times if now - t < 1.0]

        if len(self._request_times) >= self.max_per_second:
            # 计算需要等待的时间
            wait_time = 1.0 - (now - self._request_times[0])
            if wait_time > 0:
                utils.logger.debug(f"[FeishuRateLimiter] 限流，等待 {wait_time:.2f}s")
                await asyncio.sleep(wait_time)

        self._request_times.append(time.time())


class FeishuBatchWriter:
    """飞书批量写入器，攒够一批或超时后批量发送"""

    def __init__(self, token_manager: FeishuTokenManager, rate_limiter: FeishuRateLimiter):
        self.token_manager = token_manager
        self.rate_limiter = rate_limiter
        self._batch_queue: List[Dict] = []
        self._lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None

    async def add_record(self, table_id: str, fields: Dict):
        """添加一条记录到批量队列"""
        async with self._lock:
            self._batch_queue.append({
                "table_id": table_id,
                "fields": fields,
            })
            if len(self._batch_queue) >= BATCH_SIZE:
                await self._flush()
            elif self._flush_task is None:
                # 启动超时定时器
                self._flush_task = asyncio.create_task(self._delayed_flush())

    async def _delayed_flush(self):
        """延时批量写入"""
        try:
            await asyncio.sleep(BATCH_TIMEOUT)
            async with self._lock:
                if self._batch_queue:
                    await self._flush()
        finally:
            self._flush_task = None

    async def _flush(self):
        """批量写入所有队列中的记录"""
        if not self._batch_queue:
            return

        records = self._batch_queue.copy()
        self._batch_queue.clear()

        for record in records:
            await self._write_single(record["table_id"], record["fields"])

    async def _write_single(self, table_id: str, fields: Dict, retry: int = 3):
        """写入单条记录到飞书多维表格，支持重试"""
        token = await self.token_manager.get_token()
        await self.rate_limiter.acquire()

        url = f"{FEISHU_BASE_URL}/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {
            "fields": fields,
        }

        last_error = None
        for attempt in range(retry):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, json=payload) as resp:
                        data = await resp.json()
                        if data.get("code") == 0:
                            record_id = data.get("data", {}).get("record_id", "")
                            utils.logger.info(f"[FeishuStore] 写入成功 table={table_id} record_id={record_id}")
                            return
                        else:
                            last_error = f"飞书API错误: {data}"
                            utils.logger.warning(f"[FeishuStore] 写入失败 (attempt {attempt+1}/{retry}): {last_error}")
            except Exception as e:
                last_error = str(e)
                utils.logger.warning(f"[FeishuStore] 写入异常 (attempt {attempt+1}/{retry}): {e}")

            if attempt < retry - 1:
                await asyncio.sleep(1)

        utils.logger.error(f"[FeishuStore] 写入最终失败 table={table_id}: {last_error}")

    async def flush_all(self):
        """强制刷新所有未写入的数据"""
        async with self._lock:
            if self._batch_queue:
                await self._flush()


class FeishuStore(AbstractStore):
    """
    飞书多维表格 Store 实现
    
    通用实现，支持小红书/抖音/B站的数据写入。
    """

    def __init__(self, platform: str = "xhs"):
        self.platform = platform
        self.table_id = PLATFORM_TABLE_MAP.get(platform)
        self.token_manager = FeishuTokenManager()
        self.rate_limiter = FeishuRateLimiter()
        self.batch_writer = FeishuBatchWriter(self.token_manager, self.rate_limiter)
        utils.logger.info(f"[FeishuStore] 初始化 platform={platform} table_id={self.table_id}")

    def get_batch_id(self) -> str:
        """生成批次ID"""
        return str(int(time.time()))

    def timestamp_to_int(self, ts) -> int:
        """将时间戳转为int类型"""
        if ts is None:
            return 0
        try:
            return int(ts)
        except (ValueError, TypeError):
            return 0

    def extract_keyword(self, content_item: Dict) -> str:
        """从content_item中提取关键词"""
        return content_item.get("source_keyword", source_keyword_var.get() or "")

    def _build_content_fields(self, content_item: Dict) -> Dict:
        """
        构建帖子/笔记的飞书记录字段
        匹配当前飞书表结构：帖子地址、帖子标题、评论人ID、评论昵称、评论内容、评论人地区、评论时间、所属关键词、采集时间、采集批次、意向标签
        """
        desc_content = content_item.get("desc") or content_item.get("title") or ""
        fields = {
            "帖子地址": content_item.get("note_url") or content_item.get("aweme_url") or content_item.get("video_url", ""),
            "帖子标题": content_item.get("title") or content_item.get("display_title") or desc_content[:255],
            "评论人ID": content_item.get("user_id", ""),
            "评论昵称": content_item.get("nickname", ""),
            "评论内容": desc_content,
            "评论人地区": content_item.get("ip_location", ""),
            "评论时间": self.timestamp_to_int(content_item.get("create_time") or content_item.get("time", 0)),
            "所属关键词": self.extract_keyword(content_item),
            "采集时间": int(time.time()),
            "采集批次": self.get_batch_id(),
            "意向标签": "是" if check_intent(desc_content) else "否",
        }
        return fields

    def _build_comment_fields(self, comment_item: Dict) -> Dict:
        """
        构建评论的飞书记录字段
        匹配当前飞书表结构
        """
        # 兼容 tieba 字段命名
        content = comment_item.get("content", "") or comment_item.get("desc", "")
        fields = {
            "帖子地址": comment_item.get("note_url", ""),
            "帖子标题": comment_item.get("note_title", ""),
            "评论人ID": comment_item.get("user_id", ""),
            "评论昵称": comment_item.get("nickname", ""),
            "评论内容": content,
            "评论人地区": comment_item.get("ip_location", ""),
            "评论时间": self.timestamp_to_int(comment_item.get("create_time")),
            "所属关键词": self.extract_keyword(comment_item),
            "采集时间": int(time.time()),
            "采集批次": self.get_batch_id(),
            "意向标签": "是" if check_intent(content) else "否",
        }
        return fields

    async def store_content(self, content_item: Dict):
        """
        写入一条帖子/笔记数据
        """
        if not content_item:
            return

        fields = self._build_content_fields(content_item)
        await self.batch_writer.add_record(self.table_id, fields)

    async def store_comment(self, comment_item: Dict):
        """
        写入一条评论数据
        """
        if not comment_item:
            return

        fields = self._build_comment_fields(comment_item)
        await self.batch_writer.add_record(self.table_id, fields)

    async def store_creator(self, creator_item: Dict):
        """
        写入创作者信息
        """
        # 创作者信息暂不写入飞书表格
        utils.logger.debug("[FeishuStore.store_creator] 暂不写入创作者信息")
        pass

    async def flush(self):
        """强制刷新缓存数据到飞书"""
        await self.batch_writer.flush_all()
