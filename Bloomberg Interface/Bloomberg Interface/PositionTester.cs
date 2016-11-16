using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BloombergConnection;

namespace BloombergRequest
{
    class PositionTester
    {

        public static void Main(string[] args)
        {
            PositionTester tes = new PositionTester();
            string desktop = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
            string input = @"position.request";
            string output = @"position_results.csv";
            var ans = tes.GenerateTable(Path.Combine(desktop, input));
            ans = tes.PopulateTable(ans);

            using (StreamWriter write = new StreamWriter(Path.Combine(desktop, output)))
            {
                BloombergData.DataTableToCSV(ans, write, true);
            }


        }

        /// <summary>
        /// Generates base performance table
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public DataTable GenerateTable(string fileName)
        {
            DataTable table = new DataTable();
            table.TableName = "reference";
            table.Columns.Add("security", typeof(string));
            table.Columns.Add("Direction", typeof(string));
            table.Columns.Add("StartDate", typeof(DateTime));

            using (StreamReader reader = new StreamReader(fileName))
            {
                while (reader.Peek() != -1)
                {
                    string[] result = reader.ReadLine().Split(',');
                    DataRow row = table.NewRow();
                    row["security"] = result[0];
                    row["Direction"] = result[1];
                    row["StartDate"] = Convert.ToDateTime(result[2]);
                    table.Rows.Add(row);
                }
            }

            return table;
        }

        /// <summary>
        /// Adds historical values to datatable
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public DataTable PopulateTable(DataTable table)
        {

            string output = Environment.GetFolderPath(Environment.SpecialFolder.Desktop) + @"\positions_results_" + DateTime.Now.ToShortDateString().Replace('/', '-') + ".csv";
            int[] months = { 1, 3, 6, 12, 24 };
            int counter = 0;

            // for each month, 
            foreach (int month in months)
            {
                // add the column
                table.Columns.Add("CUST_TRR_RETURN_HOLDING_PER", typeof(double));

                // Get the value for each row
                foreach (DataRow row in table.Rows)
                {
                    string security = (string)row["security"];
                    DateTime start = (DateTime)row["StartDate"];
                    
                    RequestStruct request = new RequestStruct();
                    BloombergData bd = new BloombergData();

                    //format and send the bloomberg request
                    request.Type = RequestType.REFERENCE;
                    request.Period = Periodcity.DAILY;
                    request.overrides.AddRange(new List<Tuple<string, string>>
                    {
                        new Tuple<string, string>("CUST_TRR_START_DT", BloombergData.BloombergDateHelper(start)),
                        new Tuple<string, string>("CUST_TRR_END_DT", BloombergData.BloombergDateHelper(start.AddMonths(month))),
                        new Tuple<string, string>("CUST_TRR_CRNCY", "USD")
                    });

                    request.Data["fields"] = new List<string> { "CUST_TRR_RETURN_HOLDING_PER" };
                    request.Data["securities"] = new List<string> { security };

                    DataTable dummy = new DataTable();
                    dummy.TableName = "reference";
                    dummy.Columns.Add("security", typeof(string));
                    dummy.Columns.Add("CUST_TRR_RETURN_HOLDING_PER", typeof(double));

                    bd.BloombergRequest(request, dummy, null);

                    // Column offset for first 3 populated values
                    row[counter + 3] = dummy.Rows[0]["CUST_TRR_RETURN_HOLDING_PER"];
                    
                }

                // replace column name
                table.Columns["CUST_TRR_RETURN_HOLDING_PER"].ColumnName = "" + month + "mth_px";
                counter++;
            }

            // switch negatives and positives
            foreach (DataRow row in table.Rows)
            {
                string direction = (string)row["Direction"];

                if (direction.ToLower().Equals("sell"))
                {
                    for (int idx = 4; idx < table.Columns.Count; idx++)
                    {
                        row[idx] = -1 * (double)row[idx];
                    }
                }
            }

            using (StreamWriter write = new StreamWriter(output))
            {
                BloombergData.DataTableToCSV(table, write, true);
            }

            return table;
        }

    } // end of class

} // end of namespace
