### M - Money

1. Бюджет: 10 млн рублей
2. Потери без системы - 500 тысяч в месяц
3. Операц. расходы - на внешний сервер и его обслуживание - 200 тысяч в месяц
4. Срок окупаемости (без зарплат) - 33 месяцев (довольно много)
5. Без этой системы бизнес работать не будет - срок окупаемости не самое важное

### I - Ideas
1. хэшировать данные
2. Решения: (Правиловая система? Логистическая регрессия, SVM с нелинейным ядром)

### S - strategy

1. Мы работаем над глобальной стратегией банка по повышению его защищенности
2. Банк же и выделяет нам деньги и ресурсы своих сотрудников
3. Ожидания, что обрабатывать все их транзакции в режиме online и добьемся качества: FalsePositive < 5%, FalseNegative < 2%
4. Идеальный результат - отлавливаем все мошеннические транзакции

### S - skills

1. Консультант-переговорщик-лидер с опытом в данных - тот, кто намайнит из заказчика данные в наиболее оптимальном виде. И будет нести ответственность за проект, пушить заказчика.
2. Дата инжинер - кто настроит версионирование данных из банка во внешнюю систему и обратно
3. DS - кто разработает воронку + модель антифродовой системы, настроит версионирование данных и модели (mlops компетенции)

### I - input

1. Модель выдает результаты на удаленном сервере и передает их по защищенному протоколу в контур банка.
2. Её результаты это индексы мошеннических транзакций. 
3. Необходима интеграция выхода нашего сервера с туннелями банка.
4. Возможно дальнейшее улучшение модели
5. Модель надо постоянно переобучать, т.к. тактики мошенич. транзакций будут меняться.

### O - output

1. Модель выдает результаты на удаленном сервере и передает их по защищенному протоколу в контур банка.
2. Её результаты это индексы мошеннических транзакций. 
3. Необходима интеграция выхода нашего сервера с туннелями банка.
4. Возможно дальнейшее улучшение модели
5. Модель надо постоянно переобучать, т.к. тактики мошенич. транзакций будут меняться.

### N - nuances

1. Почему у банка не было такой системы до сих пор? 
2. Почему они не хотят чтобы антифрод система работала внутри банка?