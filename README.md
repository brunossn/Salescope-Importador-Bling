# Salescope-Importador-Bling
Importador de dados do ERP Bling para o Salescope.

O aplicativo cria um CSV localmente, já no layout esperado pelo Salescope Integrador, a partir dos dados de vendas localizados no Bling.

## Chamada

O importador deve ser executado com o seguintes parâmetros:

` BlingImportador.exe "Nome da Empresa" "Chave da API Bling" "Caminho completo local do CSV a ser gerado" "data inicial" `

Por exemplo:

` BlingImportador.exe "MinhaEmpresa" "b2abaacs2003d6a12a206f1e3c04" "c:\Users\MeuUsuario\Desktop\vendas.csv" 01/01/2020 `
