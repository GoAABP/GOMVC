using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using GOMVC.Data;
using GOMVC.Models;
using System.Threading.Tasks;

namespace GOMVC.Controllers
{
    public class UserController : Controller
    {
        private readonly AppDbContext _context;

        public UserController(AppDbContext context)
        {
            _context = context;
        }

        [HttpGet]
        public IActionResult Index()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Login(User userModel)
        {
            var user = await _context.Users
                                     .FirstOrDefaultAsync(u => u.USERNAME == userModel.USERNAME);

            if (user != null && user.PASSWORD == userModel.PASSWORD)
            {
                // Login successful
                return RedirectToAction("Index", "Home");
            }
            else
            {
                // Login failed
                ModelState.AddModelError(string.Empty, "Invalid login attempt.");
                return View("Index");
            }
        }
    }
}
