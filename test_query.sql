SELECT department, COUNT(*) as total, AVG(salary) as avg_salary
FROM employees 
LEFT JOIN departments ON employees.dept_id = departments.id
WHERE employees.active = TRUE AND employees.hire_date > '2020-01-01'
GROUP BY department
HAVING COUNT(*) > 5 AND AVG(salary) > 50000
QUALIFY ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) <= 3
ORDER BY total DESC