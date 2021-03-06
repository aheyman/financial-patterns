﻿using System;
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
        public Periodcity Period;
        public RequestType Type;
        public DateTime StartDate;
        public DateTime EndDate;
        public List<Tuple<string, string>> overrides = new List<Tuple<string, string>>();
        public string subType;

    }

    public enum RequestType
    {
        HISTORICAL,
        REFERENCE
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


        public BloombergData()
        {
            // Initalize the logger
            string filePath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop) + @"\Error Log\";

            if (!Directory.Exists(filePath))
                Directory.CreateDirectory(filePath);

            logFile = filePath + "log_" + DateTime.Now.ToShortDateString().Replace('/', '-') + ".txt";
        }


        /// <summary>
        /// Method to create Data Table for a request
        /// </summary>
        /// <param name="formattedData"></param>
        /// <param name="genCSV"></param>
        /// <returns></returns>
        public DataTable BloombergRequest(RequestStruct formattedData, DataTable table, string date)
        {

            string ipAddress = ConfigurationManager.AppSettings["IPAddress"];
            int port = int.Parse(ConfigurationManager.AppSettings["port"]);

            using (Session sess = StartSession(ipAddress, port, refData))
            {
                Request ans = null;
                switch (formattedData.Type)
                {
                    case RequestType.HISTORICAL:
                        ans = GenerateHistoricalRequest(sess, formattedData);
                        break;

                    case RequestType.REFERENCE:
                       ans = GenerateReferenceRequest(sess, formattedData);
                        break;
                }

                try
                {
                    sess.SendRequest(ans, new CorrelationID(1));
                    ConsumeSession(sess, table, date);
                }
                catch (Exception e)
                {
                    Logger(e.Message);
                }
                
            }


            return table;

        }

        /// <summary>
        /// Forms a Historical Request base on Data object
        /// </summary>
        /// <param name="formattedData"></param>
        /// <param name="table"></param>
        private Request GenerateHistoricalRequest(Session sess, RequestStruct formattedData)
        {

            Service refDataSvc = sess.GetService(refData);
            Request request = refDataSvc.CreateRequest("HistoricalDataRequest");
            string[] type = { "securities", "fields" };

            foreach (string str in type)
            {
                foreach (string value in formattedData.Data[str])
                    request.GetElement(str).AppendValue(value);
            }

            request.Set("startDate", BloombergDateHelper(formattedData.StartDate));
            request.Set("endDate", BloombergDateHelper(formattedData.EndDate));
            request.Set("periodicitySelection", PeriodEnumToSting(formattedData.Period));
            request.Set("nonTradingDayFillOption", "ALL_CALENDAR_DAYS");
            request.Set("nonTradingDayFillMethod", "PREVIOUS_VALUE");

            return request;

        }


        /// <summary>
        /// Formats a reference request based on a Data object
        /// </summary>
        /// <param name="formattedData"></param>
        /// <param name="table"></param>
        private Request GenerateReferenceRequest(Session sess, RequestStruct formattedData)
        {

            Service refdata = sess.GetService(refData);
            Request req = refdata.CreateRequest("ReferenceDataRequest");


            var inputs = new string[] { "securities", "fields" };

            foreach (string input in inputs)
            {
                foreach (string val in formattedData.Data[input])
                    req.GetElement(input).AppendValue(val);
            }


            Element overrides = req["overrides"];

            foreach (Tuple<string, string> tup in formattedData.overrides)
            {
                Element override1 = overrides.AppendElement();
                override1.SetElement("fieldId", tup.Item1);
                override1.SetElement("value", tup.Item2);
            }

            return req;
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
        private void ConsumeSession(Session ses, DataTable table, string date)
        {
            bool continueToLoop = true;

            while (continueToLoop)
            {
                Event eventObj = ses.NextEvent();
                switch (eventObj.Type)
                {
                    case Event.EventType.RESPONSE: // final response
                        continueToLoop = false;
                        HandleResponse(eventObj, table, date);
                        break;
                    case Event.EventType.PARTIAL_RESPONSE:
                        HandleResponse(eventObj, table, date);
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
                Logger("correlationID=" + message.CorrelationID + "\nmessageType=" + message.MessageType);                
                if (Event.EventType.SESSION_STATUS == eventObj.Type && message.MessageType.Equals("SessionTerminated"))
                {
                    Logger("Terminating: " + message.TopicName);
                    Environment.Exit(1);
                }
            }
        }

        /// <summary>
        /// Adds Bloomberg request data to a DataTable for Reference Requests
        /// </summary>
        /// <param name="eventObj"></param>
        /// <param name="table"></param>
        /// <param name="date"></param>
        private void HandleResponse(Event eventObj, DataTable table, string date)
        {

            foreach (Message message in eventObj.GetMessages()) // go through each message in the Event
            {
                //responseError
                Element DataResponse = message.AsElement;
                if (DataResponse.HasElement("responseError")) // if there is an error, quit
                {
                    Logger(DataResponse.GetElement("responseError").GetElementAsString("message"));
                    Logger("Error in the response");
                    Environment.Exit(1);
                }

                Element securityDataArray = DataResponse.GetElement("securityData");

                switch (table.TableName.ToLower())
                {
                    // For historical data repsonses, there is only one security data element
                    case "historical":
                        ProcessSecurityData(securityDataArray, table, null);
                        break;
                    case "reference":
                        for (int valueIndex = 0; valueIndex < securityDataArray.NumValues; valueIndex++)
                        {
                            Element securityData = securityDataArray.GetValueAsElement(valueIndex);
                            ProcessSecurityData(securityData, table, date);
                        }

                        break;
                }
            }// end of messages
        }

        private void ProcessSecurityData(Element securityData, DataTable table, string date)
        {
            string companyName = securityData.GetElementAsString("security");

            if (securityData.HasElement("securityError"))
            {
                Element securityError = securityData.GetElement("securityError");
                Logger("* security =" + companyName + " : " + securityError.GetElementAsString("message"));
            }
            else
            {

                Element fieldDataArray = securityData.GetElement("fieldData");
                // if the date is not null, it must be a reference request
                if (table.TableName.ToLower().Equals("reference"))
                {
                    DataRow row = table.NewRow();
                    row["security"] = companyName;

                    // Such a hack
                    if (date != null)
                    {
                        row["date"] = date;
                    }
                    for (int fieldElms = 0; fieldElms < fieldDataArray.NumElements; fieldElms++)
                    {
                        Element field = fieldDataArray.GetElement(fieldElms);
                        row[field.Name.ToString()] = field.GetValueAsString();
                    }
                    table.Rows.Add(row);
                }
                else // historical request
                {
                    for (int i = 0; i < fieldDataArray.NumValues; i++)
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
            }
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
        public List<string> GetDateRange(DateTime startDate, DateTime endDate, Periodcity period)
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
        public static string PeriodEnumToSting(Periodcity period)
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

        public static string TypeEnumToString(RequestType val)
        {
            switch (val)
            {
                case RequestType.HISTORICAL:
                    return "Historical";
                case RequestType.REFERENCE:
                    return "Reference";
                
                default:
                    throw new ArgumentException("what period did you finagle in here");
            }

        }

        /// <summary>
        /// Converts String to Periodict Enum
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static Periodcity StringToPeriodEnum(string input)
        {
            switch (input.ToLower())
            {
                case "daily":
                    return Periodcity.DAILY;
                case "weekly":
                    return Periodcity.WEEKLY;
                case "monthly":
                    return Periodcity.MONTHLY;
                case "quarterly":
                    return Periodcity.QUARTERLY;
                case "yearly":
                    return Periodcity.YEARLY;
                default:
                    throw new ArgumentException("what period did you finagle in here");
            }

        }

        /// <summary>
        /// Helper method to convert Datetime to valid Bloomberg String
        /// </summary>
        /// <param name="date"></param>
        /// <returns>YYYYMMDD Date string</returns>
        public static string BloombergDateHelper(DateTime date)
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
        public static bool DataTableToCSV(DataTable dtSource, StreamWriter writer, bool includeHeader)
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