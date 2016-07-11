using System;
using System.Collections.Generic;
using System.IO;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;

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
            Data reqData;

            bool check = hisReq.IsChecked ?? false;
            
            string overrides = overrideBox.Text;
            string tickers = tickBox.Text;
            string fields = fieldBox.Text;
            DateTime startDate = startDay.DisplayDate;
            DateTime endDate = endDay.DisplayDate;
            Periodcity period = (Periodcity)SelectedRadioValue<int>(0, rb0, rb1, rb2, rb3);



            if (check)
            {
                reqData = new HistoricalData();
                reqData.StartDate = startDate;
                reqData.EndDate = endDate;
                reqData.AddToDict("securities", ParseCSV(tickers));
                reqData.AddToDict("fields", ParseCSV(fields));
                reqData.Period = period;
            }


            else
            {
                reqData = new Reference();
                reqData.StartDate = startDate;
                reqData.EndDate = endDate;
                reqData.AddToDict("securities", ParseCSV(tickers));
                reqData.AddToDict("fields", ParseCSV(fields));
                reqData.Period = period;
                //TODO: PARSE THE Overrides
            }
                

            req.BloombergRequest(reqData);
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

        private T SelectedRadioValue<T>(T defaultValue, params RadioButton[] buttons)
        {
            foreach (RadioButton button in buttons)
            {
                if (button.IsChecked == true)
                {
                    if (button.Tag is string && typeof(T) != typeof(string))
                    {
                        string value = (string)button.Tag;
                        return (T)Convert.ChangeType(value, typeof(T));
                    }

                    return (T)button.Tag;
                }
            }

            return defaultValue;
        }

    }

}
