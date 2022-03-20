package com.atguigu.gmall.realtime.bean

case class PageStartLog(
                         province_id: String,
                         user_id: String,
                         operate_system: String,
                         channel: String,
                         is_new: String,
                         model: String,
                         mid: String,
                         version_code: String,
                         brand: String,
                         entry: String,
                         open_ad_skip_ms: Long,
                         open_ad_ms: Long,
                         loading_time: Long,
                         open_ad_id: Long,
                         ts: Long
                  )
