using System;
using System.Collections.Generic;
using System.IO;
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

    // Helper Classes
    public class RequestType
    {
        private RequestType(string value) { Value = value; }

        public string Value { get; }

        public static RequestType Historical { get { return new RequestType("HistoricalDataRequest"); } }
        public static RequestType Reference { get { return new RequestType("ReferenceDataRequest"); } }

    }

    public interface Data
    {
        Dictionary<string, List<string>> Data { get; }
        void AddToDict(string key, string value);
        void AddToDict(string key, List<string> value);
        List<Overrides> Overrides { get; }
        void AddOverrides(string fieldId, string value);
        string TypeOfRequest { get; }

    }

    public class HistoricalData : Data
    {

        public string TypeOfRequest { get { return RequestType.Historical.Value; } }

        Dictionary<string, List<string>> _dict = new Dictionary<string, List<string>>()
        {
            {"securities", new List<string>() },
            {"fields", new List<string>() },
            {"startDate", new List<string>() { "20160101" } },
            {"endDate", new List<string>() {"20160101" } },
            {"periodicitySelection", new List<string>() {"DAILY"} },
            {"nonTradingDayFillOption", new List<string>( ){"ACTIVE_DAYS_ONLY"}  },
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

    public class Reference : Data
    {

        public string TypeOfRequest { get { return RequestType.Reference.Value; } }

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



    public class BloombergData
    {

        const string refData = @"//blp/refdata";
        const string apiFields = @"//blp/apiflds";
        string logFile;


        public BloombergData()
        {
            // Initalize the logger
            string filePath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop) + @"\Error Log\";

            if (!Directory.Exists(filePath))
                Directory.CreateDirectory(filePath);

            logFile = filePath + "log_" + DateTime.Now.ToShortDateString().Replace('/', '-') + ".txt";
        }


        public void BloombergRequest(Data formattedData)
        {

            Request request;
            Session session = StartSession();
            Service refDataSvc = session.GetService(refData);
            request = refDataSvc.CreateRequest(formattedData.TypeOfRequest);

            string[] standards = { "securities", "fields" };

            foreach (string str in standards)
            {
                AddSecurityOrField(request, str, formattedData.Data[str]);
                formattedData.Data.Remove(str);
            }


            switch (formattedData.TypeOfRequest)
            {
                case "HistoricalDataRequest":

                    ProcessHistoricalRequest(request, formattedData);

                    break;

                case "ReferenceDataRequest":

                    ProcessReferenceRequest(request, formattedData);

                    break;

            }

            session.SendRequest(request, new CorrelationID(1));
            ConsumeSession(session);

        }


        private void ProcessHistoricalRequest(Request request, Data formattedData)
        {

            foreach (KeyValuePair<string, List<string>> entry in formattedData.Data)
            {
                if (entry.Value.Count > 0)
                    HistoricalSession(request, entry.Key, entry.Value.ToArray()[0]);
            }

        }


        private void ProcessReferenceRequest(Request request, Data formattedData)
        {

            foreach (Overrides over in formattedData.Overrides)
                SetOverrides(request, over);
        }


        private Session StartSession()
        {
            SessionOptions sessionOptions = new SessionOptions();
            sessionOptions.ServerHost = "localhost";
            sessionOptions.ServerPort = 8194;
            Session session = new Session(sessionOptions);
            if (!session.Start())
            {
                Logger("Could not start session.");
                Environment.Exit(1);
            }
            if (!session.OpenService("refData"))
            {
                Logger("Could not open service refData");
                Environment.Exit(1);
            }

            return session;
        }


        private void HandleResponseEvent(Event eventObj)
        {
            StringBuilder sb = new StringBuilder();
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

                //securityData
                Element securityDataArray = ReferenceDataResponse.GetElement("securityData");
                int numItems = securityDataArray.NumValues;

                for (int i = 0; i < numItems; ++i) // iterate through the list of data elents in the message
                {
                    Element securityData = securityDataArray.GetValueAsElement(i);
                    string companyName = securityData.GetElementAsString("security");
                    int sequenceNumber = securityData.GetElementAsInt32("sequenceNumber");

                    if (securityData.HasElement("securityError"))
                    {
                        Element securityError = securityData.GetElement("securityError");
                        Logger("* security =" + companyName + " : " + securityError.GetElementAsString("message"));
                    }
                    else
                    {

                        Element fieldData = securityData.GetElement("fieldData");

                        if (fieldData.HasElement("date"))
                        {
                            //TODO: process historical requests
                            //its a historical request - process acordingly
                        }
                        else
                        {
                            //TODO: process reference requests
                            //reference data request
                            for (int index = 0; index < fieldData.NumElements; index++)
                            {

                            }
                        }



                    }
                }
            }

        }


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
                    Logger("Terminating: " + message.MessageType);
                    Environment.Exit(1);
                }
            }
        }

        private void AddSecurityOrField(Request request, string key, List<string> pairToAdd)
        {
            foreach (string t in pairToAdd)
                request.GetElement(key).AppendValue(t);
        }

        private void HistoricalSession(Request request, string key, string value)
        {
            request.Set(key, value);
        }

        private void SetOverrides(Request request, Overrides over)
        {
            Element overrides = request.GetElement("overrides");
            Element override1 = overrides.AppendElement();

            override1.SetElement("fieldId", over.Field);
            override1.SetElement("value", over.Value);
        }


        private void ConsumeSession(Session ses)
        {
            bool continueToLoop = true;
            while (continueToLoop)
            {
                Event eventObj = ses.NextEvent();
                switch (eventObj.Type)
                {
                    case Event.EventType.RESPONSE: // final response
                        continueToLoop = false;
                        HandleResponseEvent(eventObj);
                        break;
                    case Event.EventType.PARTIAL_RESPONSE:
                        HandleResponseEvent(eventObj);
                        break;
                    default:
                        HandleOtherEvent(eventObj);
                        break;
                }
            }
        }

        public void Logger(string lines)
        {
            using (StreamWriter file = new StreamWriter(logFile, true))
            {
                file.Write("LOG " + DateTime.Now.ToShortTimeString() + " : " + lines);
            }
        }

    }//end of class

}//end of namespace