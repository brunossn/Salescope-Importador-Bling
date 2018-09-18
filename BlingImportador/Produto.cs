namespace BlingImportador
{
    /// <summary>
    /// Classe que representa o produto da API, para consulta do Mercadológico
    /// </summary>
    public class Produto {

        public int Id { get; set; }
        public string codigo { get; set; }
        public string descricao { get; set; }
        public string unidade { get; set; }
        public string situacao { get; set; }
        public string preco { get; set; }
        public string precoCusto { get; set; }
        public string grupoProduto { get; set; }
        public string nomeFornecedor { get; set; }
        public string marca { get; set; }
        public string pesoLiq { get; set; }
        public string dataInclusao { get; set; }
        public string dataAlteracao { get; set; }
    }
}
