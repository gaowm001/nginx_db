-- 修复 dump 恢复后序列不同步问题：dump 中的 setval 值可能小于已有主键值，
-- 导致 nextval 与现有数据冲突(如 operpermission_permissionid_seq 被设为 100/false)。
-- 这里按"序列 -> 表.列"的固定映射，把序列拨到 max(列值)。
DO $$
DECLARE
    pairs text[][] := ARRAY[
        ['operpermission_permissionid_seq','operpermission','permissionid'],
        ['rolepermission_permissionid_seq','rolepermission','permissionid'],
        ['operinfo_operatorid_seq','operinfo','operatorid'],
        ['roleinfo_roleinfoid_seq','roleinfo','roleinfoid'],
        ['orgtype_orgtypeid_seq','orgtype','orgtypeid'],
        ['province_pid_seq','province','pid'],
        ['city_cid_seq','city','cid'],
        ['sysaction_sysactionid_seq','sysaction','sysactionid'],
        ['sysorg_sysorgid_seq','sysorg','sysorgid'],
        ['systeminfo_systemid_seq','systeminfo','systemid'],
        ['loginlog_logid_seq','loginlog','logid'],
        ['serverlog_logid_seq','serverlog','logid']
    ];
    i int;
    mx bigint;
BEGIN
    FOR i IN 1..array_length(pairs,1) LOOP
        BEGIN
            EXECUTE format('SELECT max(%I) FROM sysinfo.%I', pairs[i][3], pairs[i][2]) INTO mx;
            IF mx IS NOT NULL AND mx > 0 THEN
                PERFORM setval('sysinfo.' || pairs[i][1], mx);
            END IF;
        EXCEPTION WHEN undefined_table OR undefined_column OR undefined_object THEN
            NULL; -- 缺表/缺列/缺序列时跳过
        END;
    END LOOP;
    RAISE NOTICE 'sequences synced';
END $$;
