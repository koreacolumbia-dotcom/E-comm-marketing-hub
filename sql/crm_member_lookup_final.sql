-- CRM 실제 member_id / signup_date 확인용 최종 조회
SELECT
  CAST(MemberID AS STRING) AS member_id,
  TIMESTAMP(MemberRegdate) AS signup_date,
  MemberNo,
  MemberGender,
  MemberBirthday,
  MemberJoinDevice,
  MemberOrderCount,
  MemberOrderPrice,
  MemberOrderDate
FROM `columbia-ga4.crm_raw.tb_member_staging`
WHERE MemberID IS NOT NULL
LIMIT 100;
