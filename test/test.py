import unittest
from functions.functions import get_array_conditions, orf_relationship

class TestIsDottedQad(unittest.TestCase):

    def test_orf_relationship(self):

        self.assertTrue(orf_relationship("Hola", "hola"))
        self.assertFalse(orf_relationship("Adios", "hola"))

    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)

if __name__ == '__main__':
    unittest.main()