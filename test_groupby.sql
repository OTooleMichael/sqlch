SELECT department, location, COUNT(*) as total
FROM employees
GROUP BY department, location, hire_year
ORDER BY total DESC
