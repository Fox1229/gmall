package com.atguigu.gmall.realtime.bean

case class PageLog(
                    province_id: String,
                    user_id: String,
                    operate_system: String,
                    channel: String,
                    is_new: String,
                    model: String,
                    mid: String,
                    version_code: String,
                    brand: String,
                    page_id: String,
                    page_item: String,
                    during_time: Long,
                    page_item_type: String,
                    last_page_id: String,
                    source_type: String,
                    ts: Long
                  )
