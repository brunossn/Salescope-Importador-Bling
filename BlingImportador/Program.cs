﻿using NLog;
using RestSharp;
using System;
using System.IO;
using System.Json;
using System.Linq;
using System.Text;
using BlingImportador.Helpers;
using System.Reflection;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace BlingImportador
{
    class Program
    {
        static Logger _logger;
        static string _apiKey;
        static string _nomeFilial;
        static string _dataInicial;

        const string NOME_ARQUIVO_LOG = "importador.log";
        const string LAYOUT_ARQUIVO_LOG = "${longdate}|${level}|${message} ${exception:format=tostring,data:maxInnerExceptionLevel=10:separator=\t}";
        const string ENCODING_KEY = "ISO-8859-1";

        const string URL_BASE = "https://bling.com.br/Api/v2/";
        const string URL_PEDIDO = "pedidos/page=[page]/json?filters=idSituacao[9];dataEmissao[dataInicial TO dataFinal]&apikey=[apikey]";
        const string URL_PRODUTO_ATIVO = "produtos/page=[page]/json?apikey=[apikey]&filters=situacao[A]";
        const string URL_PRODUTO_INATIVO = "produtos/page=[page]/json?apikey=[apikey]&filters=situacao[I]";
        const string URL_GRUPOPRODUTO = "gruposprodutos/json?apikey=[apikey]";

        //const string CABECALHO_KEY = "NF; Filial; Estado; Cidade; Região; Gerente; Representante; Canal; Segmento; Marca; Linha; Grupo; Subgrupo; Produto; Cliente; Coringa; Dia; Mês; Ano; Valor; Rentabilidade; Quantidade; Litros; Quilos; Metros; CNPJ - Inicio; CNPJ - Fim; Endereço; CEP; Telefone; Email; Observações do cliente";
        const string CABECALHO_KEY = "";
        const string NAO_DISPONIVEL = "ND";

        static int qtdeRequest = 0, qtdeProdutos = 0;

        /// <summary>
        /// Método inicial
        /// </summary>
        /// <param name="args">Parâmetros do programa</param>
        static void Main(string[] args)
        {
            MensagemDeAbertura();

            if (!ValidaParametros(args)) 
            {
                if (args.Contains("-d"))
                {
                    Console.WriteLine("\nPressione uma tecla para continuar ...");
                    Console.ReadKey();
                }
                return;
            }

            var argumentosSemD = args.Where(a => a != "-d").ToArray();

            _nomeFilial = argumentosSemD[0];
            _apiKey = argumentosSemD[1];
            _dataInicial = argumentosSemD[3];

            InicializarLog();
            CarregarGrupoProdutos();
            DestruirProdutos();

            _logger.Info("Carregar lista de Produtos Ativos...");
            CarregarProdutos(URL_PRODUTO_ATIVO);
            _logger.Info("Carregar lista de Produtos Ativos OK!");

            _logger.Info("Carregar lista de Produtos Inativos ...");
            CarregarProdutos(URL_PRODUTO_INATIVO);
            _logger.Info("Carregar lista de Produtos Inativos OK!");

            _logger.Info("Iniciando importação...");
            GerarArquivo(argumentosSemD[2]);
            _logger.Info("Importação finalizada, arquivo gerado com sucesso");

            try
            {
            }
            catch (Exception e)
            {
                _logger.Error(e, "Erro ao realizar a importação: ");
            }

            // Parâmetro -d. Indica execução em Daemon
            if (args.Contains("-d"))
            {
                Console.WriteLine("Importação concluída, pressione qualquer tecla para continuar...");
                Console.ReadKey();
            }
        }

        private static void MensagemDeAbertura()
        {
            var versao = typeof(Program).Assembly.GetName().Version.ToString();
            Console.WriteLine($"Integrador Bling, versão {versao}\n");
        }

        private static bool ValidaParametros(string[] args)
        {
            const int ARGUMENTOS_ESPERADOS = 4;

            var argumentosSemD = args.Where(a => a != "-d").ToArray();

            // Número de argumentos
            if (argumentosSemD.Count() != ARGUMENTOS_ESPERADOS)
            {
                Console.WriteLine("Execute novamente o importador passando parâmetros, no seguinte formato:");
                Console.WriteLine("BlingImportador.exe [nome da filial] [token do cliente no Bling] [caminho do CSV que será gerado] [data inicial].");
                Console.WriteLine("Exemplo: BlingImportador.exe b1dbaad81019d6js8ad00si \"c:\\Salescope\\Saida.csv\" 01/01/2010");
                return false;
            }

            // Diretório de saída
            var diretorio = Path.GetDirectoryName(argumentosSemD[2]);

            if(!Directory.Exists(diretorio))
            {
                try
                {
                    Directory.CreateDirectory(diretorio);
                }
                catch
                {
                    Console.WriteLine($"Não foi possível criar o diretório {diretorio}");
                }
            }

            return true;
        }

        /// <summary>
        /// Método que carrega os produtos, pois na consulta de pedidos da API Rest, os itens não carregam os dados mercadológicos
        /// </summary>
        private static void CarregarGrupoProdutos() {
            _logger.Info("Carregar Grupo Produtos...");

            // Gravando os produtos em uma solução NoSQL local, para evitar realizar muitas consultas na API Rest.
            var banco = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "app.db");
            using (var db = new LiteDB.LiteDatabase(banco)) {
                IRestResponse response;

                // A cada importação, a lista de produtos é destruida para sempre pegar uma listagem nova
                db.DropCollection("gruposprodutos");
                var gruposProdutosDB = db.GetCollection<GrupoProduto>("gruposprodutos");

                var client = new RestClient((URL_BASE + URL_GRUPOPRODUTO).Replace("[apikey]", _apiKey));
                var request = new RestRequest(Method.GET);
                response = client.Execute(request);

                // Contando os requests para logar. Isso vai ajudar a mensurar quantas requisições estão sendo feitas por execução
                qtdeRequest++;

                if (response.StatusCode == System.Net.HttpStatusCode.OK && JsonValue.Parse(response.Content)["retorno"].ContainsKey("gruposprodutos")) {
                    var listGruposProdutosJson = (JsonArray)JsonValue.Parse(response.Content)["retorno"]["gruposprodutos"];

                    foreach (var grupoProdutoAux in listGruposProdutosJson) {
                            
                        var grupoProduto = new GrupoProduto {
                            Id = grupoProdutoAux["id"],
                            nome = (string)grupoProdutoAux["nome"],
                            idPai = grupoProdutoAux["idPai"],
                            nomePai = grupoProdutoAux["nomePai"],
                        };
                        gruposProdutosDB.Insert(grupoProduto);
                    }
                }
            }

            _logger.Info("Lista de Grupos Produtos carregada");
        }

        /// <summary>
        /// Método que consulta um produtos para acessar seus dados mercadológicos
        /// </summary>
        /// <param name="codigo">Código do produto oriundo dos itens do pedido</param>
        /// <returns>Produto Consultado</returns>
        private static GrupoProduto ConsultaGrupoProduto(string codigo) {
            // Acessando a base de dados de produtos
            var banco = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "app.db");
            using (var db = new LiteDB.LiteDatabase(banco)) {
                var gruposProdutosDB = db.GetCollection<GrupoProduto>("gruposprodutos");
                var grupoProduto = gruposProdutosDB.Find(p => p.Id.Equals(codigo)).FirstOrDefault();
                return grupoProduto;
            }
        }

        private static void DestruirProdutos() {
            // Gravando os produtos em uma solução NoSQL local, para evitar realizar muitas consultas na API Rest.
            var banco = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "app.db");
            using (var db = new LiteDB.LiteDatabase(banco)) {
                // A cada importação, a lista de produtos é destruida para sempre pegar uma listagem nova
                db.DropCollection("produtos");
            }
        }

        /// <summary>
        /// Método que carrega os produtos, pois na consulta de pedidos da API Rest, os itens não carregam os dados mercadológicos
        /// </summary>
        private static void CarregarProdutos(string _URLPRODUTO)
        {
            // Paginação
            int page = 1;

            _logger.Info($"{_URLPRODUTO}");

            // Gravando os produtos em uma solução NoSQL local, para evitar realizar muitas consultas na API Rest.
            var banco = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "app.db");
            using (var db = new LiteDB.LiteDatabase(banco))
            {
                IRestResponse response;
                var produtosDB = db.GetCollection<Produto>("produtos");

                do
                {
                    _logger.Info($"Carregando produtos página {page}...");

                    var client = new RestClient((URL_BASE + _URLPRODUTO).Replace("[page]", page.ToString()).Replace("[apikey]", _apiKey));
                    var request = new RestRequest(Method.GET);
                    response = client.Execute(request);

                    // Contando os requests para logar. Isso vai ajudar a mensurar quantas requisições estão sendo feitas por execução
                    qtdeRequest++;

                    if (response.StatusCode == System.Net.HttpStatusCode.OK && JsonValue.Parse(response.Content)["retorno"].ContainsKey("produtos"))
                    {
                        var listProdutosJson = (JsonArray)JsonValue.Parse(response.Content)["retorno"]["produtos"];

                        foreach (var linha in listProdutosJson)
                        {
                            var produtoAux = linha["produto"];

                            var produto = new Produto
                            {
                                codigo = produtoAux["codigo"],
                                descricao = (string)produtoAux["descricao"],
                                unidade = produtoAux["unidade"],
                                grupoProduto = produtoAux["grupoProduto"],
                                situacao = produtoAux["situacao"],
                                nomeFornecedor = produtoAux["nomeFornecedor"],
                                marca = produtoAux["marca"],
                                preco = produtoAux["preco"],
                                precoCusto = produtoAux["precoCusto"],
                                pesoLiq = produtoAux["pesoLiq"],
                                dataInclusao = produtoAux["dataInclusao"],
                                dataAlteracao = produtoAux["dataAlteracao"]
                            };
                            produtosDB.Insert(produto);
                        }
                    }
                    _logger.Info($"Produtos da página {page} carregados");
                    page++;
                } while (response.StatusCode == System.Net.HttpStatusCode.OK && JsonValue.Parse(response.Content)["retorno"].ContainsKey("produtos")); // Continua enquanto houver produto
            }
        }

        /// <summary>
        /// Método que consulta um produtos para acessar seus dados mercadológicos
        /// </summary>
        /// <param name="codigo">Código do produto oriundo dos itens do pedido</param>
        /// <returns>Produto Consultado</returns>
        private static Produto ConsultaProduto(string codigo)
        {
            // Acessando a base de dados de produtos
            var banco = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "app.db");
            using (var db = new LiteDB.LiteDatabase(banco))
            {
                var produtosDB = db.GetCollection<Produto>("produtos");
                var produto = produtosDB.Find(p => p.codigo.Equals(codigo)).FirstOrDefault();
                return produto;
            }
        }

        /// <summary>
        /// Método que realiza a criação do arquivo e gravação dos registros
        /// </summary>
        private static void GerarArquivo(string arquivoCSV)
        {
            // A princípio, esta informação é para gerar o arquivo. Deverá ser usado na paginação, para determinar uma data de corte na consulta
            var dtImportacao = DateTime.Now;

            // Criando o arquivo por data, e com encoding do windows
            if (!File.Exists(arquivoCSV))
            {
                var arquivo = File.Create(arquivoCSV);
                arquivo.Close();
            }

            using (var sw = new StreamWriter(arquivoCSV, false, Encoding.GetEncoding(ENCODING_KEY)))
            {
                // Paginação
                int page = 1;

                // Objetos para manipulação da api rest.
                RestClient client = null;
                RestRequest request = null;
                IRestResponse response = null; // objeto responsavel por realizar a consulta;

                // Cria a primeira linha do registro que é o cabecalho
                sw.WriteLine(CABECALHO_KEY);

                // Realizando a paginação
                do
                {
                    // Inicializando o request na api rest, para cada página
                    var url = (URL_BASE + URL_PEDIDO)
                        .Replace("[page]", page.ToString())
                        .Replace("[apikey]", _apiKey)
                        .Replace("dataInicial", _dataInicial)
                        .Replace("dataFinal", $"{DateTime.Today.Day}/{DateTime.Today.Month}/{DateTime.Today.Year}");

                    client = new RestClient(url);
                    request = new RestRequest(Method.GET);
                    request.AddHeader("content-type", "application/json;charset=utf-8");
                    response = client.Execute(request);

                    // Contando os requests para logar. Isso vai ajudar a mensurar quantas requisições estão sendo feitas por execução
                    qtdeRequest++;

                    if (response.StatusCode == System.Net.HttpStatusCode.OK && JsonValue.Parse(response.Content)["retorno"].ContainsKey("pedidos"))
                    {
                        _logger.Info($"Importando pedidos (página {page})...");

                        // Acessando a lista de pedidos do json retornado  
                        dynamic listPedidosJson = JsonValue.Parse(response.Content)["retorno"]["pedidos"];

                        foreach (var linha in listPedidosJson)
                        {
                            dynamic pedido = linha["pedido"];
                            string registro = ExtrairRegistroFormatoSalescope(pedido);
                            if(!String.IsNullOrEmpty(registro)) sw.WriteLine(registro);
                        }

                        _logger.Info($"Pedidos da página {page} importados com sucesso");
                        page++; // Incrementando a paginação, pois a lista não retorna o número de paginas. Deve varrer até não poder mais.
                    }
                } while (response.StatusCode == System.Net.HttpStatusCode.OK && JsonValue.Parse(response.Content)["retorno"].ContainsKey("pedidos")); // Finalizando quando não houver mais pedidos
            }
        }

        /// <summary>
        /// Método que extraí os dados necessários para a criação do arquivo
        /// </summary>
        /// <param name="pedido">Recebe um JSON dinamico de pedido</param>
        /// <returns>A string concatenada com os registros desse pedido</returns>
        private static string ExtrairRegistroFormatoSalescope(dynamic pedido)
        {
            // Inicializando os objetos
            string registro = "";
            dynamic cliente = pedido["cliente"];
            dynamic notaFiscal = ((JsonObject)pedido).Keys.Contains("nota") ? pedido["nota"] : null;

            if (!((JsonObject)pedido).Keys.Contains("itens")) 
                return null;

            dynamic itens = pedido["itens"];

            // AS notas com status 3 = Cancelada, 5 = Rejeitada ou 10 = Denegada devem ser descartadas.
            var situacoesDescartadas = new List<string>() { "3", "5", "10" };
            if (notaFiscal != null && situacoesDescartadas.Contains(notaFiscal["situacao"]?.ToString() ?? ""))
                return null;

            // Ignorar clientes em branco
            if (cliente == null || string.IsNullOrWhiteSpace((string)cliente["cnpj"]))
            {
                if(notaFiscal == null)
                    _logger.Error($"Cliente do pedido {(string)pedido["numero"]} não encontrado.");
                else
                    _logger.Error($"Cliente da nota {notaFiscal["numero"]} não encontrado.");
                return null;
            }

            // Obtendo os dados comuns por pedido ou nota
            var nf = notaFiscal == null ? (string)pedido["numero"] : ((string)notaFiscal["numero"]).Left(10);
            var filial = _nomeFilial;
            var estado = (string)cliente["uf"];
            var cidade = ((string)cliente["cidade"]).Left(50);
            var situacao = notaFiscal == null ? "Pedido em aberto" : "Nota emitida";
            var gerente = "";
            var representante = pedido["vendedor"] != null ? (string)pedido["vendedor"] : "";
            var canal = "";
            var segmento = "";
            var nomeCliente = ((string)cliente["nome"]).Left(50);
            var complementar = "";
            var dataEmissao = notaFiscal == null ? Convert.ToDateTime((string)pedido["data"]) : Convert.ToDateTime((string)notaFiscal["dataEmissao"]);
            var endereco = String.Format("{0}, {1}, {2}", (string)cliente["endereco"], (string)cliente["bairro"], ((string)cliente["numero"]).Left(150));
            var cep = (string)cliente["cep"];
            var telefone = String.IsNullOrEmpty((string)cliente["fone"]) ? NAO_DISPONIVEL : ((string)cliente["fone"]).Replace("(", "").Replace(")", "").Replace("-", "").Replace(" ", "");
            var email = String.IsNullOrEmpty((string)cliente["email"]) ? NAO_DISPONIVEL : ((string)cliente["email"]).Left(100);
            
            var qtdeTotalItens = QtdeTotalPedido(itens);
            var valorFrete = (decimal)pedido["valorfrete"];
            var valorFretePorItem = valorFrete / qtdeTotalItens;

            var transportadora = "";
            var tipoFrete = "";

            if (!((JsonObject)pedido).Keys.Contains("transporte")) {
                transportadora = "Retirada";
                tipoFrete = "ND";
            }
            else if (((JsonObject)pedido["transporte"]).Keys.Contains("servico_correios")){
                transportadora = (string)pedido["transporte"]["servico_correios"];
                tipoFrete = "FOB";
            }
            else {
                transportadora = (string)pedido["transporte"]["transportadora"];
                tipoFrete = (string)pedido["transporte"]["tipo_frete"] == "D" ? "FOB" : "CIF";
            }

            var auxFormaPgto = "";
            if (!((JsonObject)pedido).Keys.Contains("parcelas")) {
                auxFormaPgto = "Não Informado";
            } else {
                auxFormaPgto = (string)pedido["parcelas"][0]["parcela"]["forma_pagamento"]["descricao"];
            }

            var formaPgto = "";

            if (auxFormaPgto.ToUpper().Contains("BOLETO")) {
                formaPgto = "Boleto";
            }
            else if (auxFormaPgto.ToUpper().Contains("DINHEIRO")) {
                formaPgto = "Dinheiro";
            } 
            else {
                formaPgto = "Cartão";
            }

            #region Tratamento do CNPJ
            var tipoCliente = ""; 
            string cnpjSemPonto = ((string)cliente["cnpj"]).ToString().Replace(".", "").Replace("/", "").Replace("-", "");
            string cnpjParte1 = "", cnpjParte2 = "";

            if (cnpjSemPonto.Length < 14)
                cnpjParte1 = cnpjSemPonto;
            else
            {
                cnpjParte1 = cnpjSemPonto.Substring(0, 8);
                cnpjParte2 = cnpjSemPonto.Substring(8, 4);
            }

            if(Regex.IsMatch((string)cliente["cnpj"], @"(^(\d{2}.\d{3}.\d{3}/\d{4}-\d{2})$)")){
                tipoCliente = "Pessoa Jurídica";
            }
            else if (Regex.IsMatch((string)cliente["cnpj"], @"(^(\d{3}.\d{3}.\d{3}-\d{2})$)")) {
                tipoCliente = "Pessoa Física";
            } else {
                tipoCliente = "Indefinido";
            }

            #endregion

            foreach (dynamic aux in itens)
            {
                var item = aux["item"];

                if (string.IsNullOrWhiteSpace(item["codigo"]))
                {
                    _logger.Error($"Produto '{item["descricao"]}' não possui código.");
                }
                else
                {
                    // Consultando o produto na lista pré-carregada
                    Produto produto = ConsultaProduto(item["codigo"]);
                    var nomeGrupoProduto = "";

                    if (produto != null) {
                        GrupoProduto grupoProduto = (GrupoProduto)ConsultaGrupoProduto(produto.grupoProduto);
                        nomeGrupoProduto = grupoProduto != null ? grupoProduto.nome : "";
                    }

                    // campos do produto
                    var nomeProduto = ((string)(!string.IsNullOrEmpty(produto?.descricao) ? produto?.descricao : (string)item["descricao"])).Left(50);
                    var qtde = decimal.ToInt32(Convert.ToDecimal(((string)item["quantidade"]).Replace(".", ",")));
                    var valorTotalItem = decimal.Parse(((string)item["valorunidade"]).Replace(".", ",")) * qtde;
                    var valorFreteRateado = valorFretePorItem * qtde;

                    // Criando uma linha nova para cada registro, quando já foi regitrado o primeiro item
                    if (!String.IsNullOrEmpty(registro)) registro += Environment.NewLine;

                    // Gerando a string com os dados necessários e concatenando.
                    // var stringRegistro = string.Join(";", new string[]
                    registro = string.Join(";", new string[] {
                        nf.Left(10)                                                       // Nota Fiscal ou Pedido
                        , filial.Left(50)                                                   // Filial
                        , estado.Left(2)                                                    // Estado
                        , cidade.Left(50)                                                   // Cidade
                        , situacao                                                          // Região (usado para situação)
                        , gerente.Left(50)                                                  // Gerente
                        , representante.Left(50)                                            // Representante
                        , canal.Left(50)                                                    // Canal
                        , segmento.Left(50)                                                 // Segmento
                        , produto?.marca != null ? ((string)produto.marca).Left(50) : ""     // Marca
                        , ""                                                                // Linha Produto
                        , produto?.grupoProduto != null ? ((string)produto.grupoProduto).Left(50) : "" // Produto
                        , ""                                                                // Subgrupo
                        , nomeProduto.Left(50)                                              // Descricao
                        , nomeCliente.Left(100)                                             // Cliente
                        , complementar.Left(50)                                             // Coringa
                        , dataEmissao.ToString("dd")                                        // dia
                        , dataEmissao.ToString("MM")                                        // mes
                        , dataEmissao.ToString("yyyy")                                      // ano
                        , valorTotalItem.ToString("N")                                      // valor do item
                        , valorTotalItem.ToString("N")                                      // rentabilidade
                        , qtde.ToString()                                                   // quantidade
                        , "0"                                                               // litros
                        , "0"                                                               // quilos
                        , "0"                                                               // metros
                        , cnpjParte1.Left(8)                                                // CNPJ
                        , cnpjParte2.Left(4)                                                // CNPJ_FILIAL
                        , endereco.Left(150)                                                // Endereço
                        , cep.Left(9)                                                       // CEP
                        , telefone.Left(100)                                                // Telefone
                        , email.Left(100)                                                   // Email
                        , ""                                                                // Observacoes
                        , valorFreteRateado.ToString("N")                                   // Frete - > Valor monetário adicional
                        , (valorTotalItem - valorFreteRateado).ToString("N")                // Valor monetário adicional
                        , ""                                                                // Valor monetário adicional
                        , ""                                                                // Valor monetário adicional
                        , ""                                                                // Valor monetário adicional
                        , transportadora                                                    // Transportadora --> Informação adicional de pedido
                        , tipoFrete                                                         // Tipo Frete --> Informação adicional de pedido
                        , formaPgto                                                         // Informação adicional de pedido
                        , ""                                                                // Informação adicional de pedido
                        , ""                                                                // Informação adicional de pedido
                        , ""                                                                // Informação adicional de produto
                        , ""                                                                // Informação adicional de produto
                        , ""                                                                // Informação adicional de produto
                        , ""                                                                // Informação adicional de produto
                        , ""                                                                // Informação adicional de produto
                        , tipoCliente                                                       // TipoCliente -->  Informação adicional de cliente
                        , ""                                                                // Informação adicional de cliente
                        , ""                                                                // Informação adicional de cliente
                        , ""                                                                // Informação adicional de cliente
                        , ""                                                                // Informação adicional de cliente
                    });

                    qtdeProdutos++;
                }
            }

            return registro;
        }

        private static decimal QtdeTotalPedido(dynamic itens) {
            decimal qtde = 0;

            foreach(dynamic item in itens) {
                qtde += decimal.ToInt32(Convert.ToDecimal(((string)item["item"]["quantidade"]).Replace(".", ",")));
            }

            return qtde;
        }

        /// <summary>
        /// Configuração do Log
        /// </summary>
        private static void InicializarLog()
        {
            var arquivoDeLog = new NLog.Targets.FileTarget("logfile")
            {
                FileName = NOME_ARQUIVO_LOG,
                Layout = LAYOUT_ARQUIVO_LOG
            };

            var consoleDoLog = new NLog.Targets.ConsoleTarget("logconsole")
            {
                Layout = LAYOUT_ARQUIVO_LOG
            };

            var config = new NLog.Config.LoggingConfiguration();
            config.AddRule(LogLevel.Info, LogLevel.Fatal, consoleDoLog);
            config.AddRule(LogLevel.Debug, LogLevel.Fatal, arquivoDeLog);

            LogManager.Configuration = config;

            _logger = LogManager.GetCurrentClassLogger();
        }
    }
}
