namespace BlingImportador.Helpers
{
    public static class StringExtensions{
        public static string Left(this string str, int length) {
            return str.Length < length ? str : str.Substring(0, length); 
        }
    }
}
