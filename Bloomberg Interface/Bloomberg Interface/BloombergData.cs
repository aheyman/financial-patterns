using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using CorrelationID = Bloomberglp.Blpapi.CorrelationID;
using Element = Bloomberglp.Blpapi.Element;
using Event = Bloomberglp.Blpapi.Event;
using Message = Bloomberglp.Blpapi.Message;
using Request = Bloomberglp.Blpapi.Request;
using Service = Bloomberglp.Blpapi.Service;
using Session = Bloomberglp.Blpapi.Session;
using SessionOptions = Bloomberglp.Blpapi.SessionOptions;


namespace BloombergConnection
{ 

    public class RequestStruct
    {

        public Dictionary<string, List<string>> Data = new Dictionary<string, List<string>>
        {
            {"securities", new List<string>() },
            {"fields", new List<string>() }
        };
        public List<string> securities;
        public List<string> fields;
        public Periodcity Period;
        public string RequestType;
        public DateTime StartDate;
        public DateTime EndDate;

    }

    

    public enum Periodcity
    {
        DAILY,
        WEEKLY,
        MONTHLY,
        QUARTERLY,
        YEARLY
    }


    /// <summary>
    /// Class for forming, generating and getting output
    /// </summary>
    public class BloombergData
    {

        const string refData = @"//blp/refdata";
        const string apiFields = @"//blp/apiflds";
        string output;
        string logFile;


        public BloombergData(string outputLoc)
        {
            // Initalize the logger
            string filePath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop) + @"\Error Log\";

            if (!Directory.Exists(filePath))
                Directory.CreateDirectory(filePath);

            logFile = filePath + "log_" + DateTime.Now.ToShortDateString().Replace('/', '-') + ".txt";

            if (outputLoc == null)
                output = Environment.GetFolderPath(Environment.SpecialFolder.Desktop) + @"\request_" + DateTime.Now.ToShortDateString().Replace('/', '-') + ".txt";
            else
                output = outputLoc;
        }



        public void BloombergRequest(RequestStruct formattedData)
        {

            DataTable table = new DataTable();
            table.Columns.Add("date");
            table.Columns.Add("security");

            foreach (string str in formattedData.fields )
                table.Columns.Add(str);

            switch (formattedData.RequestType.ToLower())
            {
                case "historicaldatarequest":
                    GenerateHistoricalRequest(formattedData, table);
                    break;

                case "referencedatarequest":
                    GenerateReferenceRequest(formattedData, table);
                    break;
            }

        }

        /// <summary>
        /// Forms a Historical Request base on Data object
        /// </summary>
        /// <param name="formattedData"></param>
        /// <param name="table"></param>
        public void GenerateHistoricalRequest(RequestStruct formattedData, DataTable table)
        {
            string ipAddress = ConfigurationManager.AppSettings["IPAddress"];
            int port = int.Parse(ConfigurationManager.AppSettings["port"]);

            using (Session sess = StartSession(ipAddress, port, refData))
            {
                Service refDataSvc = sess.GetService(refData);
                Request request = refDataSvc.CreateRequest("HistoricalDataRequest");

                string type = "securites";

                foreach (string str in formattedData.Data[type])
                {
                    request.GetElement(type).AppendValue(str);
                }
                request.GetElement("fields").AppendValue("PX_LAST");
                
                request.Set("startDate", BloombergDateHelper(formattedData.StartDate));
                request.Set("endDate", BloombergDateHelper(formattedData.EndDate));
                request.Set("periodicitySelection", PeriodicityHelper(formattedData.Period));
                request.Set("nonTradingDayFillOption", "NON_TRADING_WEEKDAYS");

                sess.SendRequest(request, new CorrelationID(1));

                try
                {
                    ConsumeHistSession(sess, table);
                }
                catch (Exception e)
                {
                    Logger(e.Message);
                }
                finally
                {
                    using (StreamWriter write = new StreamWriter(output))
                    {
                        DataTableToCSV(table, write, true);
                    }
                }
            }

        }


        /// <summary>
        /// Formats a reference request based on a Data object
        /// </summary>
        /// <param name="formattedData"></param>
        /// <param name="table"></param>
        private void GenerateReferenceRequest(RequestStruct formattedData, DataTable table)
        {
            List<string> daysToOverride = GetDateRange(formattedData.StartDate, formattedData.EndDate, Periodcity.QUARTERLY);

            //In the App.config file
            string ipAddress = ConfigurationManager.AppSettings["IPAddress"];
            int port = int.Parse(ConfigurationManager.AppSettings["port"]);


            using (Session sess = StartSession(ipAddress, port, refData)){
                foreach (string day in daysToOverride)
                {

                    Service refdata = sess.GetService(refData);
                    Request req = refdata.CreateRequest("ReferenceDataRequest");

                    // Securities and fields are handled same way
                    string[] standards = { "securities", "fields" };
                    foreach (string stra in standards)
                    {
                        var temp = formattedData.Data[stra];
                        foreach (string str in temp)
                        {
                            req.Set(stra, str);
                        }
                        
                    }

                    Element overrides = req["overrides"];
                    Element override1 = overrides.AppendElement();
                    override1.SetElement("FUNDAMENTAL_PUBLIC_DATE", day);

                    sess.SendRequest(req, new CorrelationID(1));

                    try
                    {
                        ConsumeRefSession(sess, table, day);
                    }
                    catch (Exception e)
                    {
                        Logger(e.Message);
                    }
                    finally
                    {
                        using (StreamWriter write = new StreamWriter(output))
                        {
                            DataTableToCSV(table, write, true);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Returns a Bloomberg Session to generate any type fo request
        /// </summary>
        /// <param name="ipAddress"></param>
        /// <param name="port"></param>
        /// <param name="requestType"></param>
        /// <returns></returns>
        private Session StartSession(string ipAddress, int port, string requestType)
        {
            SessionOptions sessionOptions = new SessionOptions();
            sessionOptions.ServerHost = ipAddress;
            sessionOptions.ServerPort = port;
            Session session = new Session(sessionOptions);
            if (!session.Start())
            {
                Logger("Could not start session.");
                Environment.Exit(1);
            }
            if (!session.OpenService(requestType))
            {
                Logger("Could not open service refData");
                Environment.Exit(1);
            }

            return session;
        }

        /// <summary>
        /// Iterates through the reference session and writes to a table
        /// Date is required because reference requests do not store dates
        /// </summary>
        /// <param name="ses"></param>
        /// <param name="table"></param>
        /// <param name="date"></param>
        private void ConsumeRefSession(Session ses, DataTable table, string date)
        {
            bool continueToLoop = true;

            while (continueToLoop)
            {
                Event eventObj = ses.NextEvent();
                switch (eventObj.Type)
                {
                    case Event.EventType.RESPONSE: // final response
                        continueToLoop = false;
                        HandleRefResponse(eventObj, table, date);
                        break;
                    case Event.EventType.PARTIAL_RESPONSE:
                        HandleRefResponse(eventObj, table, date);
                        break;
                    default:
                        HandleOtherEvent(eventObj);
                        break;
                }
            }
        }

        /// <summary>
        /// Consumes Historical Reference and writes output to a table
        /// </summary>
        /// <param name="ses"></param>
        /// <param name="table"></param>
        private void ConsumeHistSession(Session ses, DataTable table)
        {
            bool continueToLoop = true;

            while (continueToLoop)
            {
                Event eventObj = ses.NextEvent();
                switch (eventObj.Type)
                {
                    case Event.EventType.RESPONSE: // final response
                        continueToLoop = false;
                        HandleHistResponse(eventObj, table);
                        break;
                    case Event.EventType.PARTIAL_RESPONSE:
                        HandleHistResponse(eventObj, table);
                        break;
                    default:
                        HandleOtherEvent(eventObj);
                        break;
                }
            }
        }



        /// <summary>
        /// Event handler for !Event.Type.Response/Partial Response
        /// </summary>
        /// <param name="eventObj"></param>
        private void HandleOtherEvent(Event eventObj)
        {
            Logger("EventType=" + eventObj.Type);
            foreach (Message message in eventObj.GetMessages())
            {
                Logger("correlationID=" + message.CorrelationID);
                Logger("messageType=" + message.MessageType);
                if (Event.EventType.SESSION_STATUS == eventObj.Type &&
                    message.MessageType.Equals("SessionTerminated"))
                {
                    Logger("Terminating: " + message.TopicName);
                    Environment.Exit(1);
                }
            }
        }


        /// <summary>
        /// Writes bloomberg historical responses to a Datatable
        /// </summary>
        /// <param name="eventObj"></param>
        /// <param name="table"></param>
        private void HandleHistResponse(Event eventObj, DataTable table)
        {

            foreach (Message message in eventObj.GetMessages()) // go through each message in the Event
            {

                //responseError
                Element HistoricalResponse = message.AsElement;
                if (HistoricalResponse.HasElement("responseError")) // if there is an error, quit
                {
                    Logger(HistoricalResponse.GetElement("responseError").GetElementAsString("message"));
                    Logger("Error in the response");
                    Environment.Exit(1);
                }

                //securityData
                Element securityDataArray = HistoricalResponse.GetElement("securityData");


                string companyName = securityDataArray.GetElementAsString("security");
                int sequenceNumber = securityDataArray.GetElementAsInt32("sequenceNumber");

                if (securityDataArray.HasElement("securityError"))
                {
                    Element securityError = securityDataArray.GetElement("securityError");
                    Logger("* security =" + companyName + " : " + securityError.GetElementAsString("message"));
                }
                else
                {
                    Element fieldDataArray = securityDataArray.GetElement("fieldData");
                    int numItems = fieldDataArray.NumValues;

                    for (int i = 0; i < numItems; i++)
                    {
                        Element fieldData = fieldDataArray.GetValueAsElement(i);
                        DataRow row = table.NewRow();
                        row["security"] = companyName;
                        for (int k = 0; k < fieldData.NumElements; k++)
                        {
                            Element field = fieldData.GetElement(k);
                            row[field.Name.ToString()] = field.GetValueAsString();
                        }
                        table.Rows.Add(row);
                    }


                }

            }// end of messages

        }


        /// <summary>
        /// Adds Bloomberg request data to a DataTable for Reference Requests
        /// </summary>
        /// <param name="eventObj"></param>
        /// <param name="table"></param>
        /// <param name="date"></param>
        private void HandleRefResponse(Event eventObj, DataTable table, string date)
        {

            foreach (Message message in eventObj.GetMessages()) // go through each message in the Event
            {
                //responseError
                Element ReferenceDataResponse = message.AsElement;
                if (ReferenceDataResponse.HasElement("responseError")) // if there is an error, quit
                {
                    Logger(ReferenceDataResponse.GetElement("responseError").GetElementAsString("message"));
                    Logger("Error in the response");
                    Environment.Exit(1);
                }

                Element securityDataArray = ReferenceDataResponse.GetElement("securityData");
                Element securityData = securityDataArray.GetValueAsElement(0);

                string companyName = securityData.GetElementAsString("security");

                if (securityDataArray.HasElement("securityError"))
                {
                    Element securityError = securityDataArray.GetElement("securityError");
                    Logger("* security =" + companyName + " : " + securityError.GetElementAsString("message"));
                }
                else
                {

                    Element fieldData = securityData.GetElement("fieldData");

                    DataRow row = table.NewRow();
                    row["security"] = companyName;
                    row["date"] = date;

                    for (int j = 0; j < fieldData.NumElements; j++)
                    {
                        Element field = fieldData.GetElement(j);
                        row[field.Name.ToString()] = field.GetValueAsString();
                    }
                    table.Rows.Add(row);
                }


            }// end of messages

        }


        ///////////////////////////////////////////////////
        //                                              //
        //  Helper functions for formatting requests    //
        //                                              //
        //////////////////////////////////////////////////

        /// <summary>
        /// Returns a list of ALL CALENDAR DAYS (INCLUDING NON TRADING AND HOLIDAYS) between two date times
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <param name="period"></param>
        /// <returns></returns>
        private List<string> GetDateRange(DateTime startDate, DateTime endDate, Periodcity period)
        {

            List<string> result = new List<string>();

            if (endDate < startDate)
                throw new ArgumentException("endDate must be greater than or equal to startDate");

            while (startDate <= endDate)
            {

                result.Add(BloombergDateHelper(startDate));

                switch (period)
                {
                    case Periodcity.DAILY:
                        startDate = startDate.AddDays(1);
                        break;
                    case Periodcity.WEEKLY:
                        startDate = startDate.AddDays(7);
                        break;
                    case Periodcity.MONTHLY:
                        startDate = startDate.AddMonths(1);
                        break;
                    case Periodcity.QUARTERLY:
                        startDate = startDate.AddMonths(3);
                        break;
                    case Periodcity.YEARLY:
                        startDate = startDate.AddYears(1);
                        break;
                }
            }

            return result;
        }

        /// <summary>
        /// Historical Requests require strings - enum to string converter
        /// </summary>
        /// <param name="period"></param>
        /// <returns></returns>
        private string PeriodicityHelper(Periodcity period)
        {
            switch (period)
            {
                case Periodcity.DAILY:
                    return "DAILY";
                case Periodcity.WEEKLY:
                    return "WEEKLY";
                case Periodcity.MONTHLY:
                    return "MONTHLY";
                case Periodcity.QUARTERLY:
                    return "QUARTERLY";
                case Periodcity.YEARLY:
                    return "YEARLY";
                default:
                    throw new ArgumentException("what period did you finagle in here");
            }

        }

        /// <summary>
        /// Helper method to convert Datetime to valid Bloomberg String
        /// </summary>
        /// <param name="date"></param>
        /// <returns>YYYYMMDD Date string</returns>
        private string BloombergDateHelper(DateTime date)
        {
            return date.Year.ToString("D4") + date.Month.ToString("D2") + date.Day.ToString("D2");
        }


        //////////////////////////////////////////////
        //                                          //
        //  HELPER FUNCTIONS FOR OUTPUT             //
        //                                          //
        //////////////////////////////////////////////

        /// <summary>
        /// LINQ query that writes CSV to StreamWriter, stolen from SO
        /// </summary>
        /// <param name="dtSource"></param>
        /// <param name="writer"></param>
        /// <param name="includeHeader"></param>
        /// <returns>True or False if write occured</returns>
        private bool DataTableToCSV(DataTable dtSource, StreamWriter writer, bool includeHeader)
        {
            if (dtSource == null || writer == null) return false;

            if (includeHeader)
            {
                string[] columnNames = dtSource.Columns.Cast<DataColumn>().Select(column => "\"" + column.ColumnName.Replace("\"", "\"\"") + "\"").ToArray<string>();
                writer.WriteLine(string.Join(",", columnNames));
                writer.Flush();
            }

            foreach (DataRow row in dtSource.Rows)
            {
                string[] fields = row.ItemArray.Select(field => "\"" + field.ToString().Replace("\"", "\"\"") + "\"").ToArray<string>();
                writer.WriteLine(string.Join(",", fields));
                writer.Flush();
            }

            return true;
        }


        /// <summary>
        /// Simple logging utility
        /// </summary>
        /// <param name="lines"></param>
        private void Logger(string lines)
        {
            using (StreamWriter file = new StreamWriter(logFile, true))
            {
                file.Write("LOG " + DateTime.Now.ToShortTimeString() + " : " + lines + "\n");
            }
        }

    }//end of class

}//end of namespace