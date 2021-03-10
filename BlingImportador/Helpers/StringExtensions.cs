namespace BlingImportador.Helpers
{
    public static class StringExtensions{
        public static string Left(this string str, int length) {
            if (str == null) return "";
            return str.Length < length ? str : str.Substring(0, length); 
        }

        public static string Sanitize(this string dirtyString, string _replace = "") {
            //string removeChars = "?^$#@!+,;<>’\'/_*";
            string removeChars = ";";
            string result = dirtyString;

            foreach (char c in removeChars) {
                result = result.Replace(c.ToString(), _replace);
            }

            return result;
        }
    }
}
