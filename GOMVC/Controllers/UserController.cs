using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using GOMVC.Data;
using GOMVC.Models;
using System.Security.Claims;
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
                // Create the identity and principal
                var claims = new List<Claim>
                {
                    new Claim(ClaimTypes.Name, user.USERNAME)
                };
                var claimsIdentity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);
                var authProperties = new AuthenticationProperties
                {
                    IsPersistent = true,
                    ExpiresUtc = DateTimeOffset.UtcNow.AddMinutes(30) // Set cookie to expire in 30 minutes
                };

                // Sign in the user
                await HttpContext.SignInAsync(CookieAuthenticationDefaults.AuthenticationScheme, new ClaimsPrincipal(claimsIdentity), authProperties);

                // Login successful
                return RedirectToAction("Index", "Home");
            }
            else
            {
                // Login failed
                ViewData["ErrorMessage"] = "Invalid credentials.";
                return View("Index");
            }
        }


        [HttpPost]
        public async Task<IActionResult> Logout()
        {
            await HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
            return RedirectToAction("Index", "User");
        }
    }
}
