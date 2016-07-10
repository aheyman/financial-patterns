using System;
using System.Collections.Generic;
using System.IO;
using System.Windows;
using System.Windows.Controls;



namespace BloombergConnection
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        

        public MainWindow()
        {
            InitializeComponent();
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            
            var fileDialog = new System.Windows.Forms.OpenFileDialog();
            var result = fileDialog.ShowDialog();

            string sourceName = ((Button)sender).Name;
            string file;
            switch (result)
            {
                case System.Windows.Forms.DialogResult.OK:
                    file = fileDialog.FileName;
                    break;

                case System.Windows.Forms.DialogResult.Cancel:
                default:
                    file = "";
                    break;
            }

            switch (sourceName)
            {
                case "override":
                    overrideBox.Text = file;
                    break;
                case "tick":
                    tickBox.Text = file;
                    break;
                case "field":
                    fieldBox.Text = file;
                    break;
            }


        }

        private void Button_Submit(object sender, RoutedEventArgs e)
        {

            BloombergData req = new BloombergData();
            Data blah;

            bool check = hisReq.IsChecked ?? false;
            
            string overrides = overrideBox.Text;
            string tickers = tickBox.Text;
            string fields = fieldBox.Text;
            DateTime startDate = startDay.DisplayDate;
            DateTime endDate = startDay.DisplayDate;

            string bbgStartDate = BloombergDate(startDate);
            string bbgEndDate = BloombergDate(endDate);

            if (check)
            {
                blah = new HistoricalData();
                blah.AddToDict("startDate", bbgStartDate);
                blah.AddToDict("startDate", bbgEndDate);
                blah.AddToDict("securities", ParseCSV(tickers));
                blah.AddToDict("fields", ParseCSV(fields));
            }


            else
            {
                blah = new Reference();
                blah.AddToDict("securities", ParseCSV(tickers));
                blah.AddToDict("fields", ParseCSV(fields));                
            }
                

            req.BloombergRequest(blah);
        }

        private List<string> ParseCSV(string filePath)
        {
            var result = new List<string>();
            using (StreamReader reader = new StreamReader(filePath))
            {
                while(reader.Peek() != -1)
                {
                    string[] line = reader.ReadLine().Split(',');
                    foreach (string str in line)
                        result.Add(str);
                }
            }

            return result;
        }

        private string BloombergDate(DateTime dt)
        {
            return (dt.Year.ToString("D4") + dt.Month.ToString("D2") + dt.Day.ToString("D2"));
        }

    }
}
