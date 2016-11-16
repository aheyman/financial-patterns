using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BloombergConnection;
using System.IO;
using System.Data;

namespace BloombergRequest
{
    class BloombergFromFile
    {

        static void Main(string[] args)
        {
            string output = Environment.GetFolderPath(Environment.SpecialFolder.Desktop) + @"\request_" + DateTime.Now.ToShortDateString().Replace('/', '-') + ".csv";
            string desktop = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
            string input = @"rawvalues.cfg";
           
            RequestStruct request = ParseFile(Path.Combine(desktop, input));
            BloombergData data = new BloombergData();

            DataTable table = new DataTable();
            table.TableName = BloombergData.TypeEnumToString(request.Type);
            table.Columns.Add("date");
            table.Columns.Add("security");

            foreach (string str in request.Data["fields"])
                table.Columns.Add(str);

            switch (request.subType)
            {
                case "financial":
                    ReferenceRequest(data, request, table);
                    break;
                case "estimates":
                    EstimatesRequest(data, request, table);
                    break;
                default:
                    data.BloombergRequest(request, table, null);
                    break;
            }
            
            using (StreamWriter write = new StreamWriter(output))
            {
                BloombergData.DataTableToCSV(table, write, true);
            }

        }

        static public void EstimatesRequest(BloombergData data, RequestStruct request, DataTable table)
        {
            List<string> quarterOverrides = data.GetDateRange(request.StartDate, request.EndDate, Periodcity.QUARTERLY);

            string[] quarters = { "Q1", "Q2", "Q3", "Q4" };

            foreach (string day in quarterOverrides)
            {
                foreach (string quarter in quarters)
                {
                    request.overrides.AddRange(new List<Tuple<string, string>>
                    {
                        new Tuple<string, string>("BEST_FPERIOD_OVERRIDE", day.Substring(2,2)+quarter),
                    });
                    data.BloombergRequest(request, table, day.Substring(0,4)+quarter);
                }
            }
        }

        static public void ReferenceRequest(BloombergData data, RequestStruct request, DataTable table)
        {
            List<string> daysToOverride = data.GetDateRange(request.StartDate, request.EndDate, Periodcity.QUARTERLY);

            foreach (string day in daysToOverride)
            {
                request.overrides.AddRange(new List<Tuple<string, string>>
                    {
                        new Tuple<string, string>("FUNDAMENTAL_PUBLIC_DATE", day),
                        new Tuple<string, string>("FUND_PER", "Q"),
                    });
                data.BloombergRequest(request, table, day);
            }
        }

        /// <summary>
        /// Parses a simple key=value for requests
        /// </summary>
        /// <param name="inputFile"></param>
        /// <returns></returns>
        static RequestStruct ParseFile(string inputFile)
        {
            using (StreamReader reader = new StreamReader(inputFile))
            {
                string[] line = reader.ReadLine().Split('=');

                if (line.Length != 2)
                    throw new ArgumentException("File not formatted correctly");
                else
                {
                    RequestStruct input = new RequestStruct();
                    switch (line[1].ToLower())
                    {
                        case "historical":
                            input.Type = RequestType.HISTORICAL;
                            break;
                        case "reference":
                            input.Type = RequestType.REFERENCE;
                            break;
                        default:
                            throw new ArgumentException("First line must be request type");
                    }

                    while (reader.Peek() != -1)
                    {
                        line = reader.ReadLine().Split('=');
                        switch (line[0].ToLower())
                        {
                            case "startdate":
                                input.StartDate = Convert.ToDateTime(line[1]);
                                break;
                            case "enddate":
                                input.EndDate = Convert.ToDateTime(line[1]);
                                break;
                            case "securities":
                                input.Data["securities"] = line[1].Split(',').ToList();
                                break;
                            case "fields":
                                input.Data["fields"] = line[1].Split(',').ToList();
                                break;
                            case "periodicity":
                                input.Period = BloombergData.StringToPeriodEnum(line[1]);
                                break;
                            case "subtype":
                                input.subType = line[1];
                                break;
                            default:
                                throw new ArgumentException("unknown key");
                        }
                    }
                    return input;
                }
            }
        }
    } // end of class
} // end of namespace
