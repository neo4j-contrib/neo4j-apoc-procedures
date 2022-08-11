WITH
  $SubID as subid,
  $Account_Number as account_number,
  $AccountType as account_type,
  apoc.text.upperCamelCase($ActionType) as status,
  $MDN as mdn,
//  date(r.Text_Date) as enroll_date,
  case when $Offering = 'TMP MD' then date($Text_Date) else date(trim(left(case when size($Enroll_Date)=0 then $Text_Date else $Enroll_Date end,10))) end as enroll_date,
//  date(trim(left(case when size($Enroll_Date)=0 then $Text_Date else $Enroll_Date end,10))) as enroll_date,
  $Offering as offering,
  $Product_SKU as product_sku,
  $Device_Model as device_model,
  $Device_Make as device_make,
  $First_Name as first_name,
  $Last_Name as last_name,
  $Postal_CD as postal_cd,
  $City as city,
  $State as state,
  $BillingStatus as billing_status,
  $Email1 as email,
  $Email2 as email2,
  $Email3 as email3,
  case
    when ($AccountType = 'ME') then 'E'
    when ($AccountType IN ['CI','DD','DJ','FA','FL','FM','FS','FT','FW','FX','HR','LG','SG','SN','SS','US','UU','WH','FG'] and $Source = 'VerizonB2BSnapshot') then 'G'
    when (NOT $AccountType IN ['CI','DD','DJ','FA','FL','FM','FS','FT','FW','FX','HR','LG','SG','SN','SS','US','UU','WH','FG'] and $Source = 'VerizonB2BSnapshot') then 'B'
    else 'I'
  end as sub_type,
  date($Text_Date) as transaction_date

OPTIONAL MATCH (po:Offering{subid:subid})<-[p_o:CURRENT_OFFERING]-(:Subscriber)
OPTIONAL MATCH (pd:Device{subid:subid})<-[p_d:HAS_DEVICE]-(:Subscriber)
OPTIONAL MATCH (pp:Pii{subid:subid})<-[p_p:HAS_PII]-(:Subscriber)
OPTIONAL MATCH (pm:Mdn{mdn:mdn})-[p_m:MDN_USER]-(:Subscriber)
WITH subid, account_number, account_type, status, mdn, enroll_date, offering, product_sku, device_model, device_make, first_name,
    last_name, postal_cd, city, state, billing_status, email, email2, email3, sub_type,
    transaction_date, po.name  as prev_offering, po, p_o, pd, p_d,pp, p_p, pm, p_m, pm.mdn as prev_mdn,
    case
        when status = 'Add' then status
        when status = 'Drop' then status
        when status ='Update' then
            case when po.name is null then 'Add'
              when po.name = 'WPP' and offering = 'TEC' then 'Transfer'
              when po.name = 'TEC' and offering = 'WPP' then 'Transfer'
              when po.name = 'TMP MD' and offering = 'TMP' then 'Transfer'
              when po.name = 'TMP' and offering = 'TMP MD' then 'Transfer'
              when po.name IN ['WPP','TEC'] and offering IN ['TMP MD','TMP'] then 'Upgrade'
              when po.name IN ['TMP MD','TMP'] and offering IN ['WPP','TEC'] then 'Downgrade'
              else 'NA' end
        when status = 'React' then
            case when po.name is null then status
              when (offering = po.name) and (duration.inDays(po.drop_date,enroll_date).days <= 90) then 'ReActivateMe'
              when (offering = po.name) and (duration.inDays(po.drop_date,enroll_date).days > 90) then 'React>90'
              when (offering <> po.name) and (duration.inDays(po.drop_date,enroll_date).days <= 90) then 'React<=90'
              when (offering <> po.name) and (duration.inDays(po.drop_date,enroll_date).days > 90) then 'React<=90'
              else status end
        else
            status
        end as add_type,
        case
          when status = 'Add' then 1
          when status = 'Drop' then 0
          when status ='Update' then
            case when po.name is null then 1
              when po.name = 'WPP' and offering = 'TEC' then 1
              when po.name = 'TEC' and offering = 'WPP' then 1
              when po.name = 'TMP MD' and offering = 'TMP' then 1
              when po.name = 'TMP' and offering = 'TMP MD' then 1
              when po.name IN ['WPP','TEC'] and offering IN ['TMP MD','TMP'] then 1
              when po.name IN ['TMP MD','TMP'] and offering IN ['WPP','TEC'] then 1
              else 0 end
          when status = 'React' then
            case when po.name is null then 1
              when (offering = po.name) and (duration.inDays(po.drop_date,enroll_date).days <= 90) then 0
              when (offering = po.name) and (duration.inDays(po.drop_date,enroll_date).days > 90) then 1
              when (offering <> po.name) and (duration.inDays(po.drop_date,enroll_date).days <= 90) then 1
              when (offering <> po.name) and (duration.inDays(po.drop_date,enroll_date).days > 90) then 1
              else 0 end
          else
            status
        end as new_node_offering,
        coalesce(first_name,'')+coalesce(last_name,'')+coalesce(email,'')+coalesce(email2,'')+coalesce(email3,'')+coalesce(postal_cd,'')+coalesce(city,'')+coalesce(state,'') as pii,
        coalesce(pp.first_name,'')+coalesce(pp.last_name,'')+coalesce(pp.email,'')+coalesce(pp.email2,'')+coalesce(pp.email3,'')+coalesce(pp.postal_cd,'')+coalesce(pp.city,'')+coalesce(pp.state,'') as prev_pii

with  subid, account_number, mdn, account_type, status,  enroll_date, offering, product_sku, device_model,
        device_make, first_name, last_name, postal_cd, city, state, billing_status, email, email2, email3, sub_type,
        transaction_date, prev_offering, add_type, new_node_offering,
        case
            when status = 'Add' then 1
            when status = 'Drop' then 0
            when pd is null then 1
	          when status IN ['Update','React'] and pd.model <> device_model then 1 else 0
        end as new_node_device,
        case when pii<>prev_pii  and status <> 'Drop' then 1 else 0 end as new_node_pii,
        case when pm.subid is null and status <> 'Drop' then 1 when pm.subid<>subid and status <> 'Drop' then 1 else 0 end as new_node_mdn
        ,po, p_o, pd, p_d,pp, p_p, pm, p_m
order by transaction_date

//Create Account & Subscriber
MERGE (a:Account{account_number:account_number})
MERGE (s:Subscriber {subid:subid}) ON CREATE SET s.account_number = account_number, s.sub_type = sub_type
MERGE (a)-[:HAS_SUBSCRIBER]->(s)

//Drops(set the Drop Date and Drop Flag) & Reacts<=90(remove Drop Date & Drop Flag in case of same offering)
FOREACH(counter IN CASE WHEN add_type IN ['ReActivateMe','Drop'] THEN [1] ELSE [] END |
             SET po.drop_date = enroll_date,
                po.drop_flag = add_type,
                po.transaction_dt = transaction_date
        )

FOREACH(counter IN CASE WHEN NOT add_type IN ['ReActivateMe','Drop'] THEN [1] ELSE [] END |
  //Offering Adds, Reacts>90, Reacts<=90(in case of offering chagnes)
  FOREACH(counter IN CASE WHEN new_node_offering = 1 THEN [1] ELSE [] END |
    MERGE(o:Offering{subid:subid, name:offering, event_date:enroll_date})
    ON CREATE SET
      o.add_type = add_type,
      o.product_sku = product_sku,
      o.billing_status = billing_status,
      o.transaction_dt = transaction_date,
      o.first_enroll_date = enroll_date
    ON MATCH SET
      o.product_sku = product_sku,
      o.billing_status = billing_status,
      o.transaction_dt = transaction_date

    MERGE (s:Subscriber{subid:o.subid})
    MERGE (s)-[:HAS_OFFERING]->(o)

    FOREACH(counter IN CASE WHEN new_node_offering = 1 THEN [1] ELSE [] END |
      MERGE (s)-[:CURRENT_OFFERING]->(o))
    FOREACH(counter IN CASE WHEN new_node_offering = 1 AND po is NULL THEN [1] ELSE [] END |
      MERGE (s)-[:FIRST_OFFERING]->(o))
    FOREACH(counter IN CASE WHEN new_node_offering = 1 AND po IS NOT null AND po <> o THEN [1] ELSE [] END |
      MERGE (po)-[:NEXT]->(o)
      SET po.drop_date = o.enroll_date, po.drop_flag = o.add_type, o.first_enroll_date = po.first_enroll_date
      DELETE p_o)
  )
  //Devices
  FOREACH(counter IN CASE WHEN new_node_device = 1 THEN [1] ELSE [] END |
    MERGE(d:Device{subid:subid})
    SET
			d.event_date = enroll_date,
      d.make = device_make,
      d.model = device_model,
      d.prev_make = case when pd is not null then pd.make end,
      d.prev_model = case when pd is not null then pd.model end,
      d.transaction_dt = transaction_date

    MERGE (s:Subscriber{subid:d.subid})
    MERGE (s)-[:HAS_DEVICE]->(d)
  )
  //PII
  FOREACH(counter IN CASE WHEN new_node_pii = 1 THEN [1] ELSE [] END |
    MERGE(p:Pii{subid:subid})
    SET
      p.first_name = first_name,
      p.last_name = last_name,
      p.email = email,
      p.email2 = email2,
      p.email3 = email3,
      p.postal_cd = postal_cd,
      p.city = city,
      p.state = state,
      p.transaction_dt = transaction_date

    MERGE (s:Subscriber{subid:p.subid})
    MERGE (s)-[:HAS_PII]->(p)
  )
  //MDN
  FOREACH(counter IN CASE WHEN new_node_mdn = 1 THEN [1] ELSE [] END |
    MERGE(m:Mdn{subid:subid, mdn:mdn})
    MERGE (s:Subscriber{subid:m.subid})
    MERGE (m)-[:MDN_USER{mdn_date:enroll_date}]->(s)
  )
)
RETURN count(*) as total