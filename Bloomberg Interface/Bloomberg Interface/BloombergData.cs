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
    /// <summary>
    /// Interface for Requests
    /// </summary>
    public interface Data
    {
        Periodcity Period { get; set; }
        DateTime StartDate { get; set; }
        DateTime EndDate { get; set; }
        Dictionary<string, List<string>> Data { get; }
        void AddToDict(string key, string value);
        void AddToDict(string key, List<string> value);
        List<Overrides> Overrides { get; }
        void AddOverrides(string fieldId, string value);
        string TypeOfRequest { get; }

    }

    /// <summary>
    /// Class for submitting Bloomberg Historical Requests
    /// </summary>
    public class HistoricalData : Data
    {
        DateTime _start;
        public DateTime StartDate { get { return _start; } set { _start = value; } }


        DateTime _end;
        public DateTime EndDate { get { return _end; } set { _end = value; } }

        public string TypeOfRequest { get { return RequestType.Historical.Value; } }

        private Periodcity _period;
        public Periodcity Period { get { return _period; } set { _period = value; } }

        Dictionary<string, List<string>> _dict = new Dictionary<string, List<string>>()
        {
            {"securities", new List<string>() },
            {"fields", new List<string>() },
            {"nonTradingDayFillOption", new List<string>( ){"ACTIVE_DAYS_ONLY"}  },
            //There are more that should be added
        };

        public Dictionary<string, List<string>> Data { get { return _dict; } }

        public void AddToDict(string key, string value)
        {
            if (key.Contains("securities") || key.Contains("fields"))
                _dict[key].Add(value);
            else
                _dict[key][0] = value;
        }

        public void AddToDict(string key, List<string> value)
        {
            if (key.Contains("securities") || key.Contains("fields"))
                _dict[key].AddRange(value);
            else
                _dict[key][0] = value[0];
        }

        public List<Overrides> Overrides { get { return null; } }

        public void AddOverrides(string fieldId, string value)
        {
            throw new FieldAccessException("Historical Request can't be overriden");
        }


    }

    /// <summary>
    /// Class for Submitting Bloomberg Reference Requests
    /// </summary>
    public class Reference : Data
    {
        DateTime _start;
        public DateTime StartDate { get { return _start; } set { _start = value; } }

        DateTime _end;
        public DateTime EndDate { get { return _end; } set { _end = value; } }

        public string TypeOfRequest { get { return RequestType.Reference.Value; } }

        private Periodcity _period;
        public Periodcity Period { get { return _period; } set { _period = value; } }

        Dictionary<string, List<string>> _dict = new Dictionary<string, List<string>>()
        {
            {"securities", new List<string>() },
            {"fields", new List<string>() },
        };
        public Dictionary<string, List<string>> Data { get { return _dict; } }

        List<Overrides> _overrides = new List<Overrides>();
        public List<Overrides> Overrides { get { return _overrides; } }

        public void AddToDict(string key, string value)
        {
            _dict[key].Add(value);
        }

        public void AddToDict(string key, List<string> value)
        {
            _dict[key].AddRange(value);
        }

        public void AddOverrides(string fieldId, string value)
        {
            _overrides.Add(new Overrides(fieldId, value));

        }

    }

    // Helper Classes
    public class RequestType
    {
        private RequestType(string value) { Value = value; }

        public string Value { get; }

        public static RequestType Historical { get { return new RequestType("HistoricalDataRequest"); } }
        public static RequestType Reference { get { return new RequestType("ReferenceDataRequest"); } }

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
    /// Essential a well-organized tuple
    /// </summary>
    public class Overrides
    {
        private string _fieldId;
        public string Field { get { return _fieldId; } set { _fieldId = value; } }
        private string _valueId;
        public string Value { get { return _valueId; } set { _valueId = value; } }

        public Overrides(string field, string value)
        {
            _fieldId = field;
            _valueId = value;
        }
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


        public BloombergData()
        {
            // Initalize the logger
            string filePath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop) + @"\Error Log\";

            if (!Directory.Exists(filePath))
                Directory.CreateDirectory(filePath);

            logFile = filePath + "log_" + DateTime.Now.ToShortDateString().Replace('/', '-') + ".txt";
            output = Environment.GetFolderPath(Environment.SpecialFolder.Desktop) + @"\request_" + DateTime.Now.ToShortDateString().Replace('/', '-') + ".txt";
        }

        /// <summary>
        /// Allows for a user to specify output
        /// </summary>
        /// <param name="outputLoc"></param>
        public BloombergData(string outputLoc)
        {
            // Initalize the logger
            string filePath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop) + @"\Error Log\";

            if (!Directory.Exists(filePath))
                Directory.CreateDirectory(filePath);

            logFile = filePath + "log_" + DateTime.Now.ToShortDateString().Replace('/', '-') + ".txt";
            output = outputLoc;
        }



        public void BloombergRequest(Data formattedData)
        {

            DataTable table = new DataTable();
            table.Columns.Add("date");
            table.Columns.Add("security");

            foreach (string str in formattedData.Data["fields"])
                table.Columns.Add(str);

            switch (formattedData.TypeOfRequest)
            {
                case "HistoricalDataRequest":
                    GenerateHistoricalRequest(formattedData, table);
                    break;

                case "ReferenceDataRequest":
                    GenerateReferenceRequest(formattedData, table);
                    break;
            }

        }

        /// <summary>
        /// Forms a Historical Request base on Data object
        /// </summary>
        /// <param name="formattedData"></param>
        /// <param name="table"></param>
        public void GenerateHistoricalRequest(Data formattedData, DataTable table)
        {
            string ipAddress = ConfigurationManager.AppSettings["IPAddress"];
            int port = int.Parse(ConfigurationManager.AppSettings["port"]);

            using (Session sess = StartSession(ipAddress, port, refData))
            {
                Service refDataSvc = sess.GetService(refData);
                Request request = refDataSvc.CreateRequest(formattedData.TypeOfRequest);

                // Securities and fields are handled same way
                string[] standards = { "securities", "fields" };
                foreach (string str in standards)
                {
                    AddSecurityOrField(request, str, formattedData.Data[str]);
                    formattedData.Data.Remove(str);
                }
                
                //Any other dictionary values
                foreach (KeyValuePair<string, List<string>> entry in formattedData.Data)
                {
                    if (entry.Value.Count > 0)
                        HistoricalSession(request, entry.Key, entry.Value.ToArray()[0]);
                }
                HistoricalSession(request, "startDate", BloombergDateHelper(formattedData.StartDate));
                HistoricalSession(request, "endDate", BloombergDateHelper(formattedData.EndDate));
                HistoricalSession(request, "periodicitySelection", PeriodicityHelper(formattedData.Period));

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
        private void GenerateReferenceRequest(Data formattedData, DataTable table)
        {
            List<string> daysToOverride = GetDateRange(formattedData.StartDate, formattedData.EndDate, Periodcity.QUARTERLY);

            string ipAddress = ConfigurationManager.AppSettings["IPAddress"];
            int port = int.Parse(ConfigurationManager.AppSettings["port"]);

            foreach (string str in daysToOverride)
            {
                using (Session sess = StartSession(ipAddress, port, refData))
                {
                    Service refdata = sess.GetService(refData);
                    Request req = refdata.CreateRequest(formattedData.TypeOfRequest);

                    foreach (Overrides over in formattedData.Overrides)
                        SetOverrides(req, over);

                    SetOverrides(req, new Overrides("FUNDAMENTAL_PUBLIC_DATE", str));
                    sess.SendRequest(req, new CorrelationID(1));
                    try
                    {
                        ConsumeRefSession(sess, table, str);
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
        /// Helper method for adding securities and fields to requests
        /// </summary>
        /// <param name="request"></param>
        /// <param name="key"></param>
        /// <param name="pairToAdd"></param>
        private void AddSecurityOrField(Request request, string key, List<string> pairToAdd)
        {
            foreach (string t in pairToAdd)
                request.GetElement(key).AppendValue(t);
        }

        /// <summary>
        /// Helper method to set parameters for Historical Requests
        /// </summary>
        /// <param name="request"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        private void HistoricalSession(Request request, string key, string value)
        {
            request.Set(key, value);
        }

        /// <summary>
        /// Overrides are ugly and set ug-ily
        /// </summary>
        /// <param name="request"></param>
        /// <param name="over"></param>
        private void SetOverrides(Request request, Overrides over)
        {
            Element overrides = request.GetElement("overrides");
            Element override1 = overrides.AppendElement();

            override1.SetElement("fieldId", over.Field);
            override1.SetElement("value", over.Value);
        }

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
                    throw new ArgumentException("what period did you jimmy in here");
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