BEGIN READ ONLY;
SET LOCAL statement_timeout='10s';
SELECT row_to_json(t) FROM (SELECT command_id,sandbox_id,kind,state,created_at,deadline,completed_at,attempts,result_status,CASE WHEN result_status>=400 THEN left(convert_from(result_body,'UTF8'),2048) ELSE NULL END AS error_response FROM ucloud_routing_prod.gateway_commands WHERE result_status>=400 AND created_at>='2026-09-30T04:00:00Z' ORDER BY completed_at DESC LIMIT 50) t;
COMMIT;
