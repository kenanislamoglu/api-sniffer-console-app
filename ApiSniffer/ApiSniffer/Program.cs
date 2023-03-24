using FastMember;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace ApiSniffer
{
    internal class Program
    {
        static readonly string _host = "http://xxxxxxx.com/V2/api";
        static readonly string _agency = "myAgencyName";
        static readonly string _user = "myUserName";
        static readonly string _password = "myPassword";
        static readonly string _connectionString = "Data Source=myDatabaseConnectionName;Initial Catalog=myDatabaseName;Persist Security Info=True;TrustServerCertificate=True;User ID=myUserId;Password=myPassword;Connection Timeout=400";

        static void Main(string[] args)
        {
            #region get countries
            var countries = GetCountries().body.Countries;
            var countryDataTable = CountryListToDataTable(countries);
            InsertDataIntoSQLServerUsingSQLBulkCopy(countryDataTable, "myCountryTableName");
            #endregion

            #region get cities
            foreach (var country in countries)
            {
                var cities = GetCities(country.Id).body;
                if (cities == null)
                    continue;

                var locations = cities.locations;
                var arrivalDataTable = CityListToDataTable(locations, country.Id);

                AddCustomColumn(ref arrivalDataTable, "customColumn1", country.Id, typeof(System.String));
                AddCustomColumn(ref arrivalDataTable, "customColumn2", "2", typeof(System.Int32));

                InsertDataIntoSQLServerUsingSQLBulkCopy(arrivalDataTable, "myCityTableName");
            }
            #endregion
        }

        static Response<CountriesBody> GetCountries()
        {
            var token = GetToken().Result;

            var client = new RestClient(_host);
            var request = new RestRequest("productservice/GetCountries", Method.Post);
            request.AddHeader("Authorization", "Bearer " + token);
            request.AddBody(new { ProductType = 2 });
            var response = client.ExecutePost(request);

            return JsonConvert.DeserializeObject<Response<CountriesBody>>(response.Content);
        }

        static Response<CitiesBody> GetCities(string countryCode)
        {
            var token = GetToken().Result;

            var client = new RestClient(_host);
            var request = new RestRequest("service/GetCities", Method.Post);
            request.AddHeader("Authorization", "Bearer " + token);

            Root bodyContent = new Root
            {
                ProductType = 2,
                AreaLocations = new List<AreaLocation>()
            };
            bodyContent.AreaLocations.Add(new AreaLocation { Id = countryCode, Type = 2 });

            request.AddBody(bodyContent);
            var response = client.ExecutePost(request);

            return JsonConvert.DeserializeObject<Response<CitiesBody>>(response.Content);
        }

        private static async Task<string> GetToken()
        {
            var baseUrl = string.Format("{0}/authservice/login", _host);

            using (HttpClient client = new HttpClient())
            {
                var json = JsonConvert.SerializeObject(new { Agency = _agency, User = _user, Password = _password });
                var content = new StringContent(json, UnicodeEncoding.UTF8, "application/json");
                var response = await client.PostAsync(baseUrl, content);
                if (response.IsSuccessStatusCode)
                {
                    var result = await response.Content.ReadAsStringAsync();
                    var jsonObject = JObject.Parse(result);
                    return jsonObject.SelectToken("body.token").Value<string>();
                }
                else
                {
                    return "error";
                }
            };
        }

        static DataTable CountryListToDataTable(List<Country> data)
        {
            DataTable table = new DataTable();
            using (var reader = ObjectReader.Create(data, "id", "code", "name"))
            {
                table.Load(reader);
            }
            return table;
        }

        static DataTable CityListToDataTable(List<Location> data, string requestId)
        {
            DataTable table = new DataTable();
            using (var reader = ObjectReader.Create(data, "id", "isTopRegion", "name", "parentId"))
            {
                table.Load(reader);
            }

            return table;
        }

        static void AddCustomColumn(ref DataTable table, string columnName, string columnValue, Type type)
        {
            DataColumn newColumn = new DataColumn(columnName, type)
            {
                DefaultValue = columnValue
            };

            table.Columns.Add(newColumn);
        }

        static void InsertDataIntoSQLServerUsingSQLBulkCopy(DataTable fileData, string destinationTableName)
        {
            using SqlConnection dbConnection = new SqlConnection(_connectionString);
            dbConnection.Open();

            using SqlBulkCopy s = new SqlBulkCopy(dbConnection);

            s.DestinationTableName = destinationTableName;

            foreach (var column in fileData.Columns)
            {
                s.ColumnMappings.Add(column.ToString(), column.ToString());
            }

            try
            {
                s.WriteToServer(fileData);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine("Exception InsertDataIntoSQLServerUsingSQLBulkCopy:" + ex.Message);
            }
            finally { dbConnection.Close(); }
        }
    }

    #region for deserilization
    public class Response<T>
    {
        public T body { get; set; }
    }

    public class CountriesBody
    {
        public List<Country> Countries { get; set; }
    }

    public class CitiesBody
    {
        public List<Location> locations { get; set; }
    }

    public class Country
    {
        public string Id { get; set; }
        public string Code { get; set; }
        public string Name { get; set; }
    }

    public class Location
    {
        public string Id { get; set; }
        public string IsTopRegion { get; set; }
        public string Name { get; set; }
        public string ParentId { get; set; }
    }
    #endregion

    #region for request body
    public class AreaLocation
    {
        public string Id { get; set; }
        public int Type { get; set; }
    }

    public class Root
    {
        public int ProductType { get; set; }
        public List<AreaLocation> AreaLocations { get; set; }
    }
    #endregion
}