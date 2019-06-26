using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace ReplicacaoSimplificada
{
    internal static class Util
    {
        public static HashSet<string> ColumnsNotFind { get; set; }

        public static IEnumerable<DataRow> AsEnumerable(this DataTable table)
        {
            for(var i = 0; i < table.Rows.Count; i++)
                yield return table.Rows[i];
        }

        public static bool CheckColumnNameType(this DataTable dtFirst, DataTable dtSecond)
        {
            ColumnsNotFind = new HashSet<string>();

            //if(dtFirst.AsEnumerable().Select(xx => new { COLUMN_NAME = xx["COLUMN_NAME"], DATA_TYPE = xx["DATA_TYPE"] }).Except(
            //    dtSecond.AsEnumerable().Select(xx => new { COLUMN_NAME = xx["COLUMN_NAME"], DATA_TYPE = xx["DATA_TYPE"] })
            //    ).Any())
            //    return false;

            ColumnsNotFind = new HashSet<string>(dtSecond.AsEnumerable().Select(xx => new { COLUMN_NAME = xx["COLUMN_NAME"], DATA_TYPE = xx["DATA_TYPE"] }).Except(dtFirst.AsEnumerable().Select(xx => new { COLUMN_NAME = xx["COLUMN_NAME"], DATA_TYPE = xx["DATA_TYPE"] })).Select(x => x.COLUMN_NAME.ToString()));

            return ColumnsNotFind.Any();
        }

    }
}
