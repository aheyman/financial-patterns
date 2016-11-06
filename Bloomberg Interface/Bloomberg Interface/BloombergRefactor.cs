using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BloombergConnection;
using System.IO;

namespace Bloomberg_Interface
{
    class BloombergRefactor
    {

        static void Main(string[] args)
        {

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
                    switch (line[0])
                    {
                        
                        case "historical":
                            input.RequestType = "historicaldatarequest";
                            break;
                        case "reference":
                            input.RequestType = "referencedatarequest";
                            break;
                        default:
                            throw new ArgumentException("First line must be request type");

                    }

                    while (reader.Peek() != -1)
                    {

                        
                        
                    }

                    return input;
                    

                }
            }
        }
        /*

        0. What type of data is needed to generate a request

            a. once we have a requirements list, how do we form the request

            Type of request (historical or reference)
                if (historical)
                    start date, end date
                    securities ( 'US Equity')
                    fields (PX_LAST)
                    periodicity
                    request.Set("nonTradingDayFillOption", "NON_TRADING_WEEKDAYS");
                else
                    start date, end date
                    frequency (DAILY, MONTHLY, QUARTERLY, YEARLY)
                    (security)
                    (fields)
                    (overrides)


            *sample*
            type=historical
            startdate=07/1/2016
            enddate=10/01/2016
            Securities=AAPL US Equity,GOOG US Equity
            field=PX_LAST

            *sample - needs to address weekends*
            type=reference
            startdate=07/1/2016
            enddate=10/01/2016
            Securities=AAPL US Equity,GOOG US Equity
            field=TOTAL_DEBT_TO_ASSETS,BLAHBLAH,BLAH

        1. Parse in the file
        
        2. figure out how to send the request

        3. save the output to a csv file

        4. load it into a ML library



        */


    }
}
