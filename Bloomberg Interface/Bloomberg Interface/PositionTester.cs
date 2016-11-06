using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bloomberg_Interface
{
    class PositionTester
    {


        public DataTable GenerateTable(string fileName)
        {
            DataTable table = new DataTable();

            table.Columns.Add("SecurityName", typeof(string));
            table.Columns.Add("Direction", typeof(double));
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
                    table.Rows.Add(result);
                }
            }

            return table;
        }

        /*
            
            Read in list of securites:
            Security Name,  POsition Type,  Start Date,     
            AAPL,           BUY,            01/01/2016      
            ...             ....            ......          

            For each position,
                For each date in the {start, 1mth, 3mth, 6mth, 12, 24}
                    submit a bloombger request
                    Drop the result in a data table
                    
            Write to CSV
        */

        /*
            which securit(ies)?
            when did you hold it?
            which direction?

            SECURITY ..... DIRECTION ..... Cost .... Date ....... 1-mth/Price ........ 3-mth/Price ......... 6-mth/Price ....... 12-mth/Price ...... 24-mth/Price
            AAPL US EQUITY Buy/sell         ####    01/01/2013    %%%                   %%%                       %%%               %%%               %%%               
            .
            .
            .
            .
            .
            .
            BENCHMARK       Buy          ORIGINAL  Start date   blah                    blah                    blah                blah                blah

        */

    }
}
