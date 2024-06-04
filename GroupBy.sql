Select Parentesco, Count(parentesco) as Contagem From Dependentes -- Conta a quantidade de dependentes e qual é o maior numero de dependentes
Group By Parentesco; -- Filtra os tipos de parentesco ao invés de citar todos os parentescos presente na tabela Dependentes
