# Exercises 3 + 5 (`com.ex3_5`)

Exercise 5 extends the solution of Exercise 3, so both are in this package.
Change `resume()` to `restart()` in `Supervisor` to compare the two strategies.

## Exercise 3

- Write a server actor that manages a contact list that associates email addresses to names
- The server actor processes two types of messages
  - PutMsg to add a new contact in the list
  - GetMsg to read the address of a contact given its name
- Write an actor client that sends messages to the server actor and, in the case of a GetMsg, expects a reply and displays the content on the console

## Exercise 5

- Modify the solution to Exercise 3 so that the server actor fails every time a PutMsg is received with name "Fail!"
  - You may simply raise a generic Exception
- Create a supervisor actor that manages the fault
- Verify that using a resume strategy preserves the contact list, whereas a restart strategy effectively resets the contact list
