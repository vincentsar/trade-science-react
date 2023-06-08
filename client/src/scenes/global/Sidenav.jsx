import MenuOutlinedIcon from "@mui/icons-material/MenuOutlined";
import PropTypes from "prop-types";
import { Sidebar, Menu, MenuItem, SubMenu } from "react-pro-sidebar";
import { useState } from "react";

// Search related
import { styled, alpha } from "@mui/material/styles";
import SearchIcon from "@mui/icons-material/Search";
import InputBase from "@mui/material/InputBase";

// Date related
import { DatePicker } from "@mui/x-date-pickers/DatePicker";

// SEARCH RELATED --------------------------------------------------------
const Search = styled("div")(({ theme }) => ({
  position: "relative",
  borderRadius: theme.shape.borderRadius,
  backgroundColor: alpha(theme.palette.common.white, 0.5),
  "&:hover": {
    backgroundColor: alpha(theme.palette.common.white, 0.25),
  },
  marginLeft: 0,
  width: "100%",
  [theme.breakpoints.up("sm")]: {
    marginLeft: theme.spacing(1),
    width: "auto",
  },
}));

const SearchIconWrapper = styled("div")(({ theme }) => ({
  padding: theme.spacing(0, 0),
  height: "100%",
  position: "absolute",
  pointerEvents: "none",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
}));

const StyledInputBase = styled(InputBase)(({ theme }) => ({
  color: "inherit",
  "& .MuiInputBase-input": {
    padding: theme.spacing(1, 0, 1, 0),
    // vertical padding + font size from searchIcon
    paddingLeft: `calc(0.5em + ${theme.spacing(4)})`,
    transition: theme.transitions.create("width"),
    width: "100%",
    [theme.breakpoints.up("sm")]: {
      width: "12ch",
      "&:focus": {
        width: "20ch",
      },
    },
  },
}));

// MAIN NAV ITEM RELATED --------------------------------------------------------
export default function Sidenav({ data, onItemSelect }) {
  const [isCollapsed, setIsCollapsed] = useState(false);
  const [searchText, setSearchText] = useState("");
  const [dateEnd, setDateEnd] = useState("");

  console.log(`dateEnd:`);
  console.log(dateEnd);

  return (
    <div style={{ display: "flex", height: "100vh" }}>
      <Sidebar className="app" collapsed={isCollapsed}>
        <Menu>
          {/* TOGGLE COLLAPSE OR NOT BUTTON */}
          <MenuItem
            className="menu1"
            onClick={() => {
              setIsCollapsed(!isCollapsed);
              console.log(isCollapsed);
            }}
            icon={<MenuOutlinedIcon />}
          >
            <h5>TRADE WITH SCIENCE</h5>
          </MenuItem>

          {/* SEARCH BUTTON FOR RELATED SYMBOLS */}
          <MenuItem>
            <Search>
              <SearchIconWrapper>
                <SearchIcon />
              </SearchIconWrapper>
              <StyledInputBase
                placeholder="Search…"
                inputProps={{ "aria-label": "search" }}
                onChange={(e) => setSearchText(e.target.value)}
              />
            </Search>
          </MenuItem>

          {/* DATE FIELD FOR CUT OFF DATES */}
          <MenuItem>
            <DatePicker
              value={dateEnd}
              onChange={(newDate) => setDateEnd(newDate)}
              sx={{}}
            />
          </MenuItem>

          {/* LISTING OUT ALL SYMBOLS CATEGORICALLY */}
          {Object.keys(data).map((key) => {
            // If not collapsed and there is searchText
            if (!isCollapsed && searchText) {
              // Flatten and filter data, then return MenuItem components.
              return data[key]
                .filter((item) =>
                  item.toLowerCase().includes(searchText.toLowerCase())
                )
                .map((filteredItem) => (
                  <MenuItem
                    key={`${key}-${filteredItem}`}
                    onClick={() => onItemSelect(filteredItem)}
                  >
                    {`${key}/${filteredItem}`}
                  </MenuItem>
                ));
            }

            // Otherwise return the submenus as before.
            return (
              !isCollapsed && (
                <SubMenu key={key} label={key}>
                  {data[key].map((item) => (
                    <MenuItem key={item} onClick={() => onItemSelect(item)}>
                      {item}
                    </MenuItem>
                  ))}
                </SubMenu>
              )
            );
          })}
        </Menu>
      </Sidebar>
    </div>
  );
}

Sidenav.propTypes = {
  data: PropTypes.objectOf(PropTypes.arrayOf(PropTypes.string)).isRequired,
  onItemSelect: PropTypes.func.isRequired,
};
