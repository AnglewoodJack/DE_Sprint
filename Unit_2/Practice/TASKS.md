1. Чтобы успешно справиться с данным практическим заданием, вам необходимо выполнить как минимум задания 1-4 практики в теме 2.3 "Реляционные базы данных: PostgreSQL", но желательно сделать, конечно же, все. 
2. Теперь мы знакомы с гораздо большим перечнем операторов языка SQL и это дает нам дополнительные возможности для анализа данных. Выполните следующие запросы: 
* a. Попробуйте вывести не просто самую высокую зарплату во всей команде, а вывести именно фамилию сотрудника с самой высокой зарплатой. 
* b. Попробуйте вывести фамилии сотрудников в алфавитном порядке 
* c. Рассчитайте средний стаж для каждого уровня сотрудников 
* d. Выведите фамилию сотрудника и название отдела, в котором он работает 
* e. Выведите название отдела и фамилию сотрудника с самой высокой зарплатой в данном отделе и саму зарплату также. 
* f. *Выведите название отдела, сотрудники которого получат наибольшую премию по итогам года. Как рассчитать премию можно узнать в последнем задании предыдущей домашней работы 
* g. *Проиндексируйте зарплаты сотрудников с учетом коэффициента премии. Для сотрудников с коэффициентом премии больше 1.2 – размер индексации составит 20%, для сотрудников с коэффициентом премии от 1 до 1.2 размер индексации составит 10%. Для всех остальных сотрудников индексация не предусмотрена. 
* h. ***По итогам индексации отдел финансов хочет получить следующий отчет: вам необходимо на уровень каждого отдела вывести следующую информацию:
  1. Название отдела 
  2. Фамилию руководителя
  3. Количество сотрудников
  4. Средний стаж
  5. Средний уровень зарплаты
  6. Количество сотрудников уровня junior
  7. Количество сотрудников уровня middle
  8. Количество сотрудников уровня senior
  9. Количество сотрудников уровня lead
  10. Общий размер оплаты труда всех сотрудников до индексации
  11. Общий размер оплаты труда всех сотрудников после индексации
  12. Общее количество оценок А
  13. Общее количество оценок B
  14. Общее количество оценок C
  15. Общее количество оценок D
  16. Общее количество оценок Е
  17. Средний показатель коэффициента премии
  18. Общий размер премии.
  19. Общую сумму зарплат(+ премии) до индексации
  20. Общую сумму зарплат(+ премии) после индексации(премии не индексируются)
  21. Разницу в % между предыдущими двумя суммами(первая/вторая)