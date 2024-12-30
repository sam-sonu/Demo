from app.common.sysobject.sqlquerybinder import SqlQueryParamsBinder
from app.common.sysconstant.sysdbconst import DBTableConst

class SqlMapper():
    sql_binder = SqlQueryParamsBinder
        
    @classmethod
    def list_sql(cls,params):
        
        sql='''
             SELECT 
                  t.id
                , t.pid
                , t.parent_id
                , t.title
                , t.ref_link
                , (SELECT z.title FROM content_training z WHERE z.id=t.parent_id AND z.del_yn='N')  parent_training
                , t.description
                , t.trainer_id                
                , t.skill_id
                , (SELECT z.title FROM skill z WHERE z.id=t.skill_id AND z.del_yn='N')  skill
                , t.current_level_cd
                , (SELECT z.title FROM sys_code z WHERE z.code=t.current_level_cd AND z.del_yn='N')  current_level
                , t.target_level_cd
                , (SELECT z.title FROM sys_code z WHERE z.code=t.target_level_cd AND z.del_yn='N')  target_level
                , t.project_id
                , DATE(t.event_dt) event_dt
                , t.event_type_cd
                , (SELECT z.title FROM sys_code z WHERE z.code=t.event_type_cd AND z.del_yn='N')  event_type
                , t.location
                , t.user_id_arr  
                , t.show_yn              
                , r.title trainer_name
                , t.program_id
                , t.module_id
                , t.feedback_show_yn
                , IF(t.status_cd='status.draft' AND DATE(NOW()) < DATE(t.event_dt) ,'N','Y' )  completed_yn   
                , CASE
                        WHEN t.status_cd='status.draft' AND DATE(t.event_dt) > DATE(NOW()) THEN 'status.upcoming'
                        WHEN t.status_cd='status.draft' AND  DATE(NOW()) >= DATE(t.event_dt) THEN 'status.action.awaiting'
                        ELSE t.status_cd
                    END status_cd        
                , (SELECT IF(t.status_cd='status.draft' AND DATE(t.event_dt)= DATE(NOW()),'InProgress/Action Awaiting'
                            , IF(t.status_cd='status.draft' AND DATE(t.event_dt) > DATE(NOW()),'Upcoming'
                                , IF(t.status_cd='status.draft' AND  DATE(NOW()) >= DATE(t.event_dt) ,'Action Awaiting'
                                , z.title ) )
                            )  FROM sys_code z WHERE z.code=t.status_cd AND z.del_yn='N')  status
                , t.start_time 
                , t.end_time                
                , (SELECT z.title FROM sys_user z WHERE z.id=t.creator_id AND z.del_yn='N') creator_nm          
                , (SELECT z.id FROM sys_user z WHERE z.id=t.creator_id AND z.del_yn='N') creator_id    
                , CASE WHEN MAX( CASE WHEN t.creator_id={{loginId}} OR (s.member_id = {{loginId}} AND s.role_cd='training.role.coauthor') THEN 1 ELSE 0 END ) = 1 THEN 'Y' ELSE 'N' END edit_yn              
                , DATE(t.create_dt) create_dt
                , DATE(t.modify_dt) modify_dt
                , (SELECT IF(t.status_cd='status.draft' AND DATE(t.event_dt)= DATE(NOW()),5
                            , IF(t.status_cd='status.draft' AND DATE(t.event_dt)> DATE(NOW()),4
                                , 0 )
                            )  FROM sys_code z WHERE z.code=t.status_cd AND z.del_yn='N')  sno
                , COUNT(*) member_count
            FROM 
                content_training t
                JOIN sys_user r
                ON t.trainer_id = r.id
                LEFT JOIN content_training_member s
                ON t.id = s.training_id
                AND s.del_yn='N'
            WHERE 1=1
                {% if skillId %}
                AND t.skill_id ={{ skillId }} 
                {% endif %}
                AND t.del_yn='N'
                AND r.del_yn='N'
            GROUP BY t.id
            ORDER BY sno DESC, event_dt DESC
            {% if rowLimit %}
            LIMIT  {{ rowLimit }}
            {% endif %}
        '''        

        return cls.sql_binder.prepare_sql(sql, params)
    
    @classmethod
    def obj_sql(cls,params):
        
        sql='''
             SELECT 
                  t.id
                , t.pid
                , t.title
                , t.ref_link
                , (SELECT z.title FROM content_training z WHERE z.id=t.parent_id AND z.del_yn='N')  parent_training
                , t.trainer_id                
                , t.skill_id
                , (SELECT z.title FROM skill z WHERE z.id=t.skill_id AND z.del_yn='N')  skill
                , t.current_level_cd
                , (SELECT z.title FROM sys_code z WHERE z.code=t.current_level_cd AND z.del_yn='N')  current_level
                , t.target_level_cd
                , (SELECT z.title FROM sys_code z WHERE z.code=t.target_level_cd AND z.del_yn='N')  target_level
                , t.project_id
                , DATE(t.event_dt) event_dt
                , t.event_type_cd
                , (SELECT z.title FROM sys_code z WHERE z.code=t.event_type_cd AND z.del_yn='N')  event_type
                , t.location
                , t.description
                , t.user_id_arr
                , t.show_yn
                , r.title trainer_name
                , t.status_cd 
                , IF(t.status_cd='status.draft' AND DATE(NOW()) < DATE(t.event_dt) ,'N','Y' )  completed_yn   
                , CASE
                        WHEN t.status_cd='status.draft' AND DATE(t.event_dt) > DATE(NOW()) THEN 'status.upcoming'
                        WHEN t.status_cd='status.draft' AND  DATE(NOW()) >= DATE(t.event_dt) THEN 'status.action.awaiting'
                        ELSE t.status_cd
                    END status_cd         
                , (SELECT IF(t.status_cd='status.draft' AND DATE(t.event_dt)= DATE(NOW()),'InProgress/Action Awaiting'
                            , IF(t.status_cd='status.draft' AND DATE(t.event_dt) > DATE(NOW()),'Upcoming'
                                , IF(t.status_cd='status.draft' AND  DATE(NOW()) > DATE(t.event_dt) ,'Action Awaiting'
                                , z.title ) )
                            )  FROM sys_code z WHERE z.code=t.status_cd AND z.del_yn='N')  status
                
                , (SELECT COUNT(*) FROM content_training_member z WHERE z.training_id=t.id AND z.del_yn='N')  member_count  
                , ROUND((SELECT AVG((SELECT x.code_val FROM sys_code x WHERE x.code=z.rating_cd AND x.del_yn='N')) FROM content_training_member z WHERE z.training_id=t.id AND z.del_yn='N'))  avg_rating   
                , t.start_time 
                , t.end_time                
                , (SELECT z.title FROM sys_user z WHERE z.id=t.creator_id AND z.del_yn='N')  creator_nm                
                , DATE(t.create_dt) create_dt
                , DATE(t.modify_dt) modify_dt
                , IF(t.creator_id={{loginId}},'Y','N') edit_yn
            FROM 
                content_training t
                JOIN sys_user r  ON t.trainer_id = r.id
            WHERE 1=1
                AND t.id={{id}}
                AND t.del_yn='N'
        '''        

        return cls.sql_binder.prepare_sql(sql, params)