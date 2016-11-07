﻿using System;
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

            table.Columns.Add("SecurityName", typeof(string));
            table.Columns.Add("Direction", typeof(string));
            table.Columns.Add("StartDate", typeof(DateTime));
            table.Columns.Add("Cost", typeof(double));
            table.Columns.Add("1mth_px", typeof(double));
            table.Columns.Add("3mth_px", typeof(double));
            table.Columns.Add("6mth_px", typeof(double));
            table.Columns.Add("12mth_px", typeof(double));
            table.Columns.Add("24mth_px", typeof(double));

            using (StreamReader reader = new StreamReader(fileName))
            {
                while (reader.Peek() != -1)
                {
                    string[] result = reader.ReadLine().Split(',');
                    DataRow row = table.NewRow();
                    row["SecurityName"] = result[0];
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
            DataTable result;
            foreach (DataRow row in table.Rows)
            {

                int[] months = { 0, 1, 3, 6, 12, 24 };

                string security = (string)row["SecurityName"];
                DateTime start = (DateTime)row["StartDate"];
                int counter = 0;

                foreach (int month in months)
                {

                    RequestStruct request = new RequestStruct();
                    BloombergData bd = new BloombergData(null);

                    //format and send the bloomberg request
                    request.Type = RequestType.HISTORICAL;
                    request.Period = Periodcity.DAILY;
                    request.StartDate = start.AddMonths(month);
                    request.EndDate = start.AddMonths(month);
                    request.Data["fields"] = new List<string> { "PX_LAST" };
                    request.Data["securities"] = new List<string> { security };
                    result = bd.BloombergRequest(request, false);

                    // Column offset for first 3 populated values
                    row[counter + 3] = result.Rows[0]["PX_LAST"];
                    counter++;
                }
            }
            return table;
        }

    } // end of class

} // end of namespace
