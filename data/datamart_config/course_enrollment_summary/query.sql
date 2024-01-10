select
    course_category_id
    , type as course_type
    , course_id
    , title as course_title
    , student_id
    , concat(s.first_name, ' ', s.last_name) as student_name
    , city
    , mentor_id
    , concat(m.first_name, ' ', m.last_name) as mentor_name
from `dwh.dim_student` as s
join `dwh.fact_course_enrollment` as ce 
    on ce.student_id = s.id
join `dwh.fact_course` as c
    on c.id = ce.course_id
join `dwh.dim_course_category` as cc
    on cc.id = c.course_category_id 
join `dwh.dim_mentor` m 
    on c.mentor_id = m.id;