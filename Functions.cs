using Microsoft.Azure.WebJobs;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Threading.Tasks;

namespace voting_app_worker
{
    public class Functions
    {
        private static CloudStorageAccount StorageAccount = CloudStorageAccount.Parse(ConfigurationManager.ConnectionStrings["AzureWebJobsStorage"].ToString());

        public async static Task ProcessQueueMessage([QueueTrigger("votes")] string message, TextWriter logger)
        {
            dynamic record = JsonConvert.DeserializeObject(message);
            await logger.WriteLineAsync($"Processing {record.voter_id} with {record.vote}");

            CloudTableClient tableClient = StorageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference("votes");
            table.CreateIfNotExists();

            // vote 
            var voteEntity = new VoteEntity((string)record.voter_id, (string)record.vote);
            table.Execute(TableOperation.InsertOrReplace(voteEntity));

            // vote count 
            var batchOperation = new TableBatchOperation();
            foreach (var voteCount in await GetVoteCount(table))
            {
                batchOperation.InsertOrReplace(new VoteCountEntity(voteCount.Key, voteCount.Value));
            }
            table.ExecuteBatch(batchOperation);

        }

        private static async Task<IDictionary<string, int>> GetVoteCount(CloudTable table)
        {
            string filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, VoteEntity.PK);
            TableQuery<VoteEntity> tableQuery = new TableQuery<VoteEntity>().Where(filter);
            TableContinuationToken continuationToken = null;

            var voteCounts = new Dictionary<string, int>()
            {
                { "a", 0 },
                { "b", 0 },
            };

            do
            {
                var rows = await table.ExecuteQuerySegmentedAsync(tableQuery, continuationToken);
                foreach (var row in rows)
                {
                    voteCounts[row.Vote]++;
                }

                continuationToken = rows.ContinuationToken;
            } while (continuationToken != null);

            return voteCounts;
        }
    }
}
