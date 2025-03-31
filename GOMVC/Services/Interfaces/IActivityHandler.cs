using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

public interface IActivityHandler
{
    string ActivityName { get; }
    Task<IActionResult> ExecuteAsync();
}
