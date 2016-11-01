using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Version_2
{

    public enum Periodcity
    {
        DAILY,
        WEEKLY,
        MONTHLY,
        QUARTERLY,
        YEARLY
    }

    class Request
    {

        string StartDate { get; set; }
        string EndDate { get; set; }
        string RequestType { get; set; }
        Periodcity Period { get; set; }
        List<string> Securities;
        List<string> Fields;
        List<Tuple<string, string>> Overrides;



    }
}
