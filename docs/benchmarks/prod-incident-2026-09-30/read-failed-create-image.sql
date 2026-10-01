BEGIN READ ONLY;
SET LOCAL statement_timeout='10s';
SELECT json_build_object('sandbox_id',sandbox_id,'keys',(SELECT json_agg(key) FROM jsonb_object_keys(convert_from(body,'UTF8')::jsonb) AS key),'image',convert_from(body,'UTF8')::jsonb->'image','image_id',convert_from(body,'UTF8')::jsonb->'image_id','spec_image',convert_from(body,'UTF8')::jsonb->'spec'->'image','spec_image_id',convert_from(body,'UTF8')::jsonb->'spec'->'image_id') FROM ucloud_routing_prod.gateway_commands WHERE command_id='756f6410-21d2-4541-a5d9-f06069c5ca83';
COMMIT;
