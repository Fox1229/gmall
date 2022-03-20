package com.atguigu.gmall.realtime.bean

case class PageDisplayLog(
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
                           display_type:String,
                           display_item: String,
                           display_item_type:String,
                           display_pos_id:String,
                           display_order:String ,
                           ts:Long
                         )
