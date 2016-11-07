using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BloombergConnection;
using System.IO;

namespace BloombergRequest
{
    class BloombergFromFile
    {

        static void Main(string[] args)
        {
            string desktop = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
            string input = @"rawvalues.cfg";

            RequestStruct request = ParseFile(Path.Combine(desktop,input));
            BloombergData data = new BloombergData(null);
            var ans = data.BloombergRequest(request, true);

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
                                input.Data["securities"]= line[1].Split(',').ToList();
                                break;
                            case "fields":
                                input.Data["fields"] = line[1].Split(',').ToList();
                                break;
                            case "periodicity":
                                input.Period = BloombergData.StringToPeriodEnum(line[1]);
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
