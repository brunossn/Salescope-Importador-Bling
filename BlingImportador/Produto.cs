using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlingImportador {
    /// <summary>
    /// Classe que representa o produto da API, para consulta do Mercadológico
    /// </summary>
    public class Produto {

        public int Id { get; set; }
        public String codigo { get; set; }
        public String descricao { get; set; }
        public String unidade { get; set; }
        public String situacao { get; set; }
        public String preco { get; set; }
        public String precoCusto { get; set; }
        public String grupoProduto { get; set; }
        public String nomeFornecedor { get; set; }
        public String marca { get; set; }
        public String pesoLiq { get; set; }
        public String dataInclusao { get; set; }
        public String dataAlteracao { get; set; }
    }
}
