// Archivo: Common/ErrorCatalog.cs
namespace Common
{
    public static class ErrorCatalog
    {
        public const int ArchivoNoEncontrado = 1;              // "No se encontró el archivo"
        public const int ErrorConversionArchivo = 2;           // "No se pudo convertir el archivo"
        public const int ErrorBulkInsert = 3;                  // "No se pudo proceder con el bulk insert"
        public const int ErrorInsercionTablaFinal = 4;         // "No se pudo insertar a la tabla final"
        public const int ErrorMoverArchivo = 5;                // "No se pudo mover el archivo"
        public const int ErrorExportacionArchivo = 6;          // "No se pudo exportar el archivo"
        public const int ErrorGeneracionLog = 7;               // "No se pudo generar el log"
        public const int ErrorMoverLog = 8;                    // "No se pudo mover el log"
    }

    public static class ErrorMessages
    {
        public const string ArchivoNoEncontrado = "No se encontró el archivo";
        public const string ErrorConversionArchivo = "No se pudo convertir el archivo";
        public const string ErrorBulkInsert = "No se pudo proceder con el bulk insert";
        public const string ErrorInsercionTablaFinal = "No se pudo insertar a la tabla final";
        public const string ErrorMoverArchivo = "No se pudo mover el archivo";
        public const string ErrorExportacionArchivo = "No se pudo exportar el archivo";
        public const string ErrorGeneracionLog = "No se pudo generar el log";
        public const string ErrorMoverLog = "No se pudo mover el log";
    }
}
