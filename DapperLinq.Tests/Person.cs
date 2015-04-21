namespace DapperLinq.Tests
{
    public enum Sex
    {
        Female, Male
    }

    public class Person
    {        
        public Person()
        {

        }

        public int Id { get; set; }
        public string Name { get; set; }
        public double Balance { get; set; }
        public int Age { get; set; }
        public bool IsMan { get; set; }
        public Sex Sex { get; set; }
        public int CountryId { get; set; }
    }
}